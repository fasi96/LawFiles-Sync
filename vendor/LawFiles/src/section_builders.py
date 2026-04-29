"""
section_builders.py - One builder function per BCI section.

Each function takes a CSVRow, the mapping config, and a HeaderResolver,
and returns a list of lines for that section. This modular approach means
each section can be debugged/fixed independently.

The resolver translates header name strings from field_mapping.json into
column indices, so the converter works across different CSV exports.
"""

import re
from datetime import datetime
from transforms import (
    state_to_abbrev, suffix_to_generation, format_zip_10, format_zip_bci_space,
    format_zip_debtor,
    parse_other_names, parse_dependants, parse_vehicle, parse_bank_account,
    parse_investment_account, parse_creditor, parse_tax_debt, parse_lawsuit,
    parse_insider_loan, parse_seizure, parse_gift, parse_loss,
    parse_closed_account, parse_other_income, parse_other_expense,
    parse_support_recipient, format_amount, parse_pipe_list,
    format_date_leading_zeros, TRANSFORMS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(row, resolver, header_ref, default=""):
    """Get a value from a CSV row using a header reference (resolved to index)."""
    if not header_ref:
        return default
    col_idx = resolver.resolve(header_ref)
    return row.get(col_idx, default)


def _get_first(row, resolver, header_refs, default=""):
    """Try multiple header references, return first non-empty value."""
    for ref in header_refs:
        val = _get(row, resolver, ref, "")
        if val:
            return val
    return default


def _is_joint(row, resolver):
    """Detect if this is a joint filing."""
    val = _get(row, resolver, "Do you plan on filing your case individually or jointly with your spouse?")
    return val == "Joint (with your spouse)"


def _is_married(row, resolver):
    """Detect if debtor marital status is married/separated."""
    status = (_get(row, resolver, "Your Marital Status", "") or "").strip().lower()
    return status in {"married", "separated", "seperated"}


def _determine_owner(row, resolver, individual_col, joint_col):
    """
    Determine asset owner based on which checkbox column has 'Yes'.
    Returns: 1=debtor, 2=spouse, 3=joint/both
    """
    ind_val = _get(row, resolver, individual_col) if individual_col else ""
    jnt_val = _get(row, resolver, joint_col) if joint_col else ""
    if ind_val and jnt_val:
        return 3  # both checked
    if jnt_val:
        return 2 if _is_joint(row, resolver) else 1  # joint filing: spouse; individual: debtor
    if ind_val:
        return 1
    return 1  # default to debtor


def _apply_transform(value, transform_name):
    """Apply a named transform function to a value."""
    if transform_name and transform_name in TRANSFORMS:
        return TRANSFORMS[transform_name](value)
    return value


def _csv_field(value):
    """Quote a CSV field value if it contains commas."""
    if ',' in value:
        return f'"{value}"'
    return value


def _normalize_multi_value_text(value):
    """Normalize comma-delimited free text into semicolon-delimited text."""
    if not value:
        return ""
    parts = [p.strip() for p in value.split(',') if p.strip()]
    if len(parts) <= 1:
        return value.strip()
    return '; '.join(parts)


def _normalize_inline_whitespace(value):
    """Collapse tabs/newlines/multiple spaces into single spaces."""
    if not value:
        return ""
    return " ".join(str(value).split())


def _property_type_flags(prop_type):
    """Map property type string to BCI checkbox flag dict."""
    pt = prop_type.lower() if prop_type else ""
    flags = {"SingleFamilyHome": 0, "DuplexMultiUnit": 0, "CondoCoop": 0,
             "Mobile": 0, "Land": 0, "Investment": 0, "Timeshare": 0, "Other": 0}
    if "single" in pt or "home" in pt:
        flags["SingleFamilyHome"] = 1
    elif "duplex" in pt or "multi" in pt:
        flags["DuplexMultiUnit"] = 1
    elif "condo" in pt or "co-op" in pt:
        flags["CondoCoop"] = 1
    elif "mobile" in pt or "manufactured" in pt:
        flags["Mobile"] = 1
    elif "land" in pt:
        flags["Land"] = 1
    elif "investment" in pt:
        flags["Investment"] = 1
    elif "timeshare" in pt:
        flags["Timeshare"] = 1
    elif pt:
        flags["Other"] = 1
    return flags


def _format_zip_dash(value):
    """Format ZIP as XXXXX-XXXX with dash separator."""
    if not value:
        return ""
    value = value.strip().replace("-", " ")
    parts = value.split()
    zip5 = parts[0].strip() if parts else value
    zip4 = parts[1].strip() if len(parts) > 1 else ""
    if zip4:
        return f"{zip5}-{zip4}"
    return zip5


def _build_simple_kv(row, resolver, field_defs, empty_fields=None):
    """
    Build key=value lines from a field definition dict.
    field_defs: { "BCIKey": { "csv_col": "Header Name", "transform": "...", "default": "..." } }
    empty_fields: list of BCI keys that should be present but empty
    """
    lines = []
    for key, spec in field_defs.items():
        if isinstance(spec, dict):
            csv_col = spec.get("csv_col")
            transform = spec.get("transform")
            default = spec.get("default", "")
            if csv_col is not None:
                col_idx = resolver.resolve(csv_col)
                value = row.get(col_idx) if col_idx is not None else default
            else:
                value = default
            if not value and default:
                value = default
            value = _apply_transform(value, transform)
        else:
            value = spec  # plain string default
        lines.append(f"{key}={value}")

    if empty_fields:
        for key in empty_fields:
            lines.append(f"{key}=")

    return lines


# Counter for generating unique PLinkID / CLinkID values
_link_counter = 3796460


def _next_link_id():
    global _link_counter
    _link_counter += 1
    return _link_counter


def _reset_link_counter(start=3796460):
    global _link_counter
    _link_counter = start


# ---------------------------------------------------------------------------
# [File] section
# ---------------------------------------------------------------------------

def build_file_section(row, mapping, resolver):
    now = datetime.now()
    export_id_col = mapping['file_header'].get('export_id_col', 'Entry Id')
    export_id = _get(row, resolver, export_id_col, "0")
    date_str = f"{now.month}/{now.day}/{now.year}"
    lines = [
        f"Version={mapping['file_header']['defaults']['Version']}",
        f"ExportDate={date_str} 12:00:00 AM",
        f"ExportTime={int(now.timestamp() * 10000000)}",
        f"BExportVer={mapping['file_header']['defaults']['BExportVer']}",
        f"MyCaseInfo Export={mapping['file_header']['defaults']['MyCaseInfo Export']}",
        f"ExportID={export_id}",
    ]
    return lines


# ---------------------------------------------------------------------------
# [Case] section
# ---------------------------------------------------------------------------

def build_case_section(row, mapping, resolver):
    export_id_col = mapping['file_header'].get('export_id_col', 'Entry Id')
    export_id = _get(row, resolver, export_id_col, "0")
    now = datetime.now()
    lines = [
        f"CaseNotes=Exported ID {export_id} from MyCaseInfo  {now.month}/{now.day}/{now.year}  {now.strftime('%I:%M %p')}",
    ]
    for k, v in mapping["case"]["defaults"].items():
        if k == "TypeOfDebtor" and _is_joint(row, resolver):
            lines.append(f"{k}=2")
        else:
            lines.append(f"{k}={v}")
    return lines


# ---------------------------------------------------------------------------
# [Debtor] section
# ---------------------------------------------------------------------------

def build_debtor_section(row, mapping, resolver):
    cfg = mapping["debtor"]
    lines = _build_simple_kv(row, resolver, cfg["fields"])
    # Fix phone/email for different form versions
    for i, line in enumerate(lines):
        if line == "HomePhone=" or line.startswith("HomePhone="):
            val = line.split("=", 1)[1]
            if not val:
                val = _get_first(row, resolver, ["Your Phone Number", "Phone Number"])
            lines[i] = f"HomePhone={val}"
        elif line == "EMail=" or line.startswith("EMail="):
            val = line.split("=", 1)[1]
            if not val:
                val = _get_first(row, resolver, ["Your Email Address (Enter Email)", "Your Email Address"])
            lines[i] = f"EMail={val}"
    # Move apartment/suite/unit from City to Addr
    addr_idx = city_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Addr="):
            addr_idx = i
        elif line.startswith("City="):
            city_idx = i
    if addr_idx is not None and city_idx is not None:
        city_val = lines[city_idx].split("=", 1)[1]
        match = re.search(r'\s+(apt|suite|ste|unit|#)\s*', city_val, re.IGNORECASE)
        if match:
            addr_val = lines[addr_idx].split("=", 1)[1]
            lines[addr_idx] = f"Addr={addr_val} {city_val[match.start():]}"
            lines[city_idx] = f"City={city_val[:match.start()]}"
    return lines


# ---------------------------------------------------------------------------
# [Joint] section
# ---------------------------------------------------------------------------

def build_joint_section(row, mapping, resolver):
    cfg = mapping["joint"]
    lines = _build_simple_kv(row, resolver, cfg["fields"])
    joint = _is_joint(row, resolver)
    if joint:
        # For joint filing, spouse shares debtor's address
        dcfg = mapping["debtor"]["fields"]
        lines.append(f"Addr={_get(row, resolver, dcfg['Addr']['csv_col'])}")
        lines.append(f"City={_get(row, resolver, dcfg['City']['csv_col'])}")
        lines.append(f"State={state_to_abbrev(_get(row, resolver, dcfg['State']['csv_col']))}")
        lines.append(f"Zip={format_zip_debtor(_get(row, resolver, dcfg['Zip']['csv_col']))}")
    else:
        for key in (cfg.get("empty_fields") or []):
            if key in ("Addr", "City", "State", "Zip"):
                lines.append(f"{key}=")

    # Add remaining empty/default fields
    for key in (cfg.get("empty_fields") or []):
        if key in ("Addr", "City", "State", "Zip"):
            if not joint:
                continue  # already added above
            else:
                continue  # already added above
        if key == "HomePhone" and joint:
            phone = _get_first(row, resolver, ["Your Spouse's Phone Number"])
            lines.append(f"HomePhone={phone}")
        else:
            lines.append(f"{key}=")

    return lines


# ---------------------------------------------------------------------------
# [Other Names] section
# ---------------------------------------------------------------------------

def build_other_names_section(row, mapping, resolver):
    cfg = mapping["other_names"]
    raw = _get(row, resolver, cfg["csv_col"])
    all_names = []
    if raw:
        all_names.extend(parse_other_names(raw))
    # Spouse other names for joint filing
    if _is_joint(row, resolver):
        spouse_raw = _get(row, resolver, "Your Spouse's Other Name(s) 1")
        if spouse_raw:
            all_names.extend(parse_other_names(spouse_raw))
    lines = []
    for idx, (name_type, name_raw, display, middle, suffix) in enumerate(all_names, 1):
        lines.append(f"{idx},{name_type},{name_raw},{display},{middle},{suffix}")
    return lines


# ---------------------------------------------------------------------------
# [Dependant] section
# ---------------------------------------------------------------------------

def build_dependant_section(row, mapping, resolver):
    cfg = mapping["dependants"]
    joint = _is_joint(row, resolver)
    lines = []
    dep_counter = 1
    # Dynamically discover all "Household Members N" columns
    col_refs = list(cfg["csv_cols"])
    n = len(col_refs) + 1
    while True:
        extra = f"Household Members {n}"
        idx = resolver.resolve(extra)
        if idx is None:
            break
        col_refs.append(extra)
        n += 1
    _NON_CHILD_RELATIONSHIPS = {
        "wife", "husband", "spouse", "partner", "boyfriend", "girlfriend",
        "domestic partner", "significant other", "fiance", "fiancee",
        "fiancé", "fiancée",
    }
    for col_ref in col_refs:
        parsed = parse_dependants(_get(row, resolver, col_ref))
        if parsed:
            relationship, age = parsed
            if relationship.strip().lower() in _NON_CHILD_RELATIONSHIPS:
                continue
            if joint:
                # Joint filing: no name, lives_with=3, is_dependent=3
                lines.append(f"{relationship},{age},,3,3")
            else:
                dep_name = f"Dependent Name {dep_counter}"
                lives_with = cfg["default_lives_with"]
                is_dep = cfg["default_is_dependent"]
                lines.append(f"{relationship},{age},{dep_name},{lives_with},{is_dep}")
            dep_counter += 1
    return lines


# ---------------------------------------------------------------------------
# [Prior Cases] section
# ---------------------------------------------------------------------------

def _normalize_prior_date(date_val):
    """Normalize prior case date to MM/DD/YYYY format.
    Handles: '4/1/2023', 'Aug-01' (Mon-YY), 'Sep-02' etc."""
    if not date_val:
        return date_val
    from datetime import datetime as dt
    # Try full date format first
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            parsed = dt.strptime(date_val.strip(), fmt)
            return parsed.strftime("%m/%d/%Y")
        except (ValueError, TypeError):
            pass
    # Try Mon-YY format (e.g., "Aug-01" = August 2001)
    try:
        parsed = dt.strptime(date_val.strip(), "%b-%y")
        return parsed.strftime("%m/01/%Y")
    except (ValueError, TypeError):
        pass
    return date_val


def build_prior_cases_section(row, mapping, resolver):
    cfg = mapping["prior_cases"]
    lines = []
    date_val = _get(row, resolver, cfg["date_col"])
    state_val = _get(row, resolver, cfg["state_col"])
    if date_val:
        date_val = _normalize_prior_date(date_val)
        lines.append(f"{date_val},{state_val},")
    # Spouse prior cases
    s_date = _get(row, resolver, cfg.get("spouse_date_col"))
    s_state = _get(row, resolver, cfg.get("spouse_state_col"))
    if s_date:
        s_date = _normalize_prior_date(s_date)
        lines.append(f"{s_date},{s_state},")
    return lines


# ---------------------------------------------------------------------------
# [SchAB] section - Schedule A/B (Assets)
# ---------------------------------------------------------------------------

def build_schab_section(row, mapping, resolver):
    cfg = mapping["schedule_ab"]
    joint = _is_joint(row, resolver)

    header = "PLinkID,Type,Owner,MarketValue,SecuredAmt,Interest,Description,Address1,Address2,Address3,City,State,Zip,SingleFamilyHome,DuplexMultiUnit,CondoCoop,Mobile,Land,Investment,Timeshare,Other,OtherType,AddlDescription1,AddlDescription2,AddlDescription3,AddlDescription4,Community,OwnedWithAnother,Amended,OwnedPercent"
    lines = [header]
    zero_flags = ",0,0,0,0,0,,0"

    # --- Determine owner codes for joint filing ---
    # For joint: check which checkbox set has data (1st = debtor, 2nd = spouse/joint)
    def _asset_owner(ind_col, jnt_col):
        if not joint:
            return 1
        return _determine_owner(row, resolver, ind_col, jnt_col)

    # Detect checkbox positions for bank/investment/retirement/vehicles/HHG/electronics
    bank_owner = 3 if joint else _asset_owner("Bank Accounts", "Bank Accounts#2")
    inv_owner = 3 if joint else _asset_owner("Investment Accounts (Non-Retirement)", "Investment Accounts (Non-Retirement)#2")
    # For retirement, joint (mutual) filing always uses owner=3
    ret_owner = 3 if joint else 1

    # Vehicle owner: joint (mutual) filing always uses owner=3
    veh_owner = 3 if joint else 1

    # HHG/Electronics owner: joint (mutual) filing always uses owner=3
    hhg_owner = 3 if joint else 1

    # Special assets (Less Common Assets) - joint filing defaults to owner=3
    special_owner = 3 if joint else 1

    # Tax refund owner - joint filing defaults to owner=3
    refund_owner = 3 if joint else 1

    # --- Owned Home (Type 1) - if they own, not rent ---
    re_cfg = cfg["real_estate"]
    housing = _get(row, resolver, "Real Estate")
    if housing == "Own":
        # Add the owned home as real estate
        dcfg = mapping["debtor"]["fields"]
        home_addr = _get(row, resolver, dcfg["Addr"]["csv_col"])
        home_city = _get(row, resolver, dcfg["City"]["csv_col"])
        home_state = state_to_abbrev(_get(row, resolver, dcfg["State"]["csv_col"]))
        home_zip = _get(row, resolver, dcfg["Zip"]["csv_col"])
        home_zip5 = home_zip.split()[0].split("-")[0] if home_zip else ""
        home_owner = 3 if joint else 1  # Joint filing: home is jointly owned
        plid = _next_link_id()
        lines.append(f"{plid},1,{home_owner},0.00,,,,{home_addr},,,{home_city},{home_state},{home_zip5},{zero_flags},,,,,,0,0,,")

    # --- Other Real Estate (Type 1) - only if "Other Real Estate" checkbox is Yes ---
    has_other_re = _get(row, resolver, "Other Real Estate")
    if not has_other_re and joint:
        has_other_re = _get(row, resolver, "Other Real Estate#2")
    if not has_other_re or has_other_re != "Yes":
        has_other_re = ""  # Skip other real estate if checkbox not checked
    for prop in (re_cfg["properties"] if has_other_re else []):
        prop_type = _get(row, resolver, prop.get("type_col"))
        if not prop_type:
            continue
        addr = _get(row, resolver, prop.get("addr_col"), "")
        city = _get(row, resolver, prop.get("city_col"), "")
        state = state_to_abbrev(_get(row, resolver, prop.get("state_col"), ""))
        raw_zip = _get(row, resolver, prop.get("zip_col"), "")
        # Format zip as XXXXX-XXXX (dash format)
        zipcode = _format_zip_dash(raw_zip)
        desc = _get(row, resolver, prop.get("description_col"), "")
        value = "0.00"
        # Property type checkbox flags
        pt_flags = _property_type_flags(prop_type)
        description = desc if desc else prop_type
        # Mapping Helper expects an explicit residence label for timeshare entries.
        if pt_flags["Timeshare"] and (not description or description.strip().lower() == "timeshare"):
            description = "Residence: Timeshare"
        plid = _next_link_id()
        if not state and not addr:
            state = state_to_abbrev(_get(row, resolver, re_cfg.get("debtor_state_col", "Your Current Address (State / Province)"), ""))
        prop_owner = 3 if joint else 1
        lines.append(
            f"{plid},1,{prop_owner},{value},,,{description},{addr},,,{city},{state},{zipcode},"
            f"{pt_flags['SingleFamilyHome']},{pt_flags['DuplexMultiUnit']},{pt_flags['CondoCoop']},"
            f"{pt_flags['Mobile']},{pt_flags['Land']},{pt_flags['Investment']},{pt_flags['Timeshare']},"
            f"{pt_flags['Other']},,,,,,,0,0,,"
        )

    # --- Tax Refund (Type 28) ---
    tr_cfg = cfg["tax_refund"]
    refund_amt = _get_first(row, resolver, ["Pending IRS Tax Refund", "Pending IRS Tax Refund#2"])
    if refund_amt:
        plid = _next_link_id()
        lines.append(f"{plid},28,{refund_owner},{format_amount(refund_amt)},,,{tr_cfg['default_description']},,,,,,,{zero_flags},,{tr_cfg['addl_desc1']},,,,0,0,,")

    # --- Bank Accounts (Type 17) ---
    ba_cfg = cfg["bank_accounts"]
    for col_ref in ba_cfg["csv_cols"]:
        acct = parse_bank_account(_get(row, resolver, col_ref))
        if not acct:
            continue
        plid = _next_link_id()
        acct_type = acct['account_type']
        if acct_type in ("Checking", "Savings"):
            acct_type_full = f"{acct_type} Account"
        else:
            acct_type_full = acct_type
        desc = f"{acct_type_full}: {acct['bank_name']} {acct['last4']}"
        lines.append(f"{plid},17,{bank_owner},{ba_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,{acct_type_full},,,,0,0,,")

    # --- Investment Accounts (Type 18) ---
    ia_cfg = cfg["investment_accounts"]
    for col_ref in ia_cfg["csv_cols"]:
        acct = parse_investment_account(_get(row, resolver, col_ref))
        if not acct:
            continue
        plid = _next_link_id()
        desc = f"Investment Account: {acct['name']} {acct['account_number']}"
        lines.append(f"{plid},18,{inv_owner},{ia_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Retirement, Education IRA, Annuities (Types 21, 23, 24) ---
    ret_cfg = cfg["retirement_accounts"]
    annuities = []
    education_iras = []
    retirements = []
    for col_ref in ret_cfg["csv_cols"]:
        acct = parse_investment_account(_get(row, resolver, col_ref))
        if not acct:
            continue
        name_lower = acct["name"].lower()
        if "annuity" in name_lower or "annuit" in name_lower:
            annuities.append(acct)
        elif "education" in name_lower:
            education_iras.append(acct)
        else:
            retirements.append(acct)

    for acct in annuities:
        plid = _next_link_id()
        desc = f"Annuities: {acct['name']} {acct['account_number']}"
        lines.append(f"{plid},23,{ret_owner},{ret_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    for acct in education_iras:
        plid = _next_link_id()
        desc = f"Education IRA: {acct['name']} {acct['account_number']}"
        lines.append(f"{plid},24,{ret_owner},{ret_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    for acct in retirements:
        plid = _next_link_id()
        acct_type = acct.get("account_type", "")
        name_lower = f"{acct_type} {acct['name']}".lower()
        addl_desc = ""
        for keyword, code in ret_cfg["retirement_type_map"].items():
            if keyword.lower() in name_lower:
                addl_desc = code
                break
        if acct_type:
            desc = f"{acct_type} {acct['name']} {acct['account_number']}".strip()
        else:
            desc = f"{acct['name']} {acct['account_number']}".strip()
        lines.append(f"{plid},21,{ret_owner},{ret_cfg['default_value']},,{ret_cfg['interest']},{desc},,,,,,,{zero_flags},,{addl_desc},,,,0,0,,")

    # --- Business Interests (Type 19) ---
    # Create Type 19 entries for each business the debtor/spouse owns
    biz_name_cols = [
        ("What's the name of your business?", "What's the nature of your business?",
         "Business Assets", "Business Debts & Liabilities", "Business Current Value"),
    ]
    for name_col, nature_col, assets_col, liabilities_col, value_col in biz_name_cols:
        biz_name = _get(row, resolver, name_col)
        if not biz_name:
            continue
        biz_nature = _get(row, resolver, nature_col, "")
        biz_assets = _get(row, resolver, assets_col, "")
        biz_liabilities = _get(row, resolver, liabilities_col, "")
        biz_value = _get(row, resolver, value_col, "")
        plid = _next_link_id()
        desc = f"Business Interest: {biz_name}"
        owner = 3 if joint else 1
        mkt_value = format_amount(biz_value) if biz_value else "1.00"
        lines.append(f"{plid},19,{owner},{mkt_value},,,{_csv_field(desc)},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Undeposited Funds (Type 20) ---
    # Check Undeposited Funds #2 column (joint/spouse set)
    undep_val = _get(row, resolver, "Undeposited Funds#2")
    if not undep_val:
        undep_val = _get(row, resolver, "Undeposited Funds")
    if undep_val:
        plid = _next_link_id()
        owner = 3 if joint else 1
        lines.append(f"{plid},20,{owner},0.00,,,{_csv_field(undep_val)},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Insurance (Type 31) ---
    ins_cfg = cfg["insurance"]
    ins_val = _get(row, resolver, ins_cfg["checkbox_col"])
    # For joint, also check spouse checkbox set
    ins_val_spouse = ""
    if joint:
        ins_val_spouse = _get(row, resolver, "You or your spouse own a Whole Life Insurance Policy which lists you as both the \"insured\" and the \"beneficiary\"")
    if ins_val or ins_val_spouse:
        plid = _next_link_id()
        if ins_val_spouse and not ins_val:
            desc = "Insurance: Whole Life Insurance Policy which lists you as both the insured and the beneficiary"
        else:
            desc = "Insurance: You own a Whole Life Insurance Policy which lists you as both the insured and the beneficiary"
        owner = special_owner
        lines.append(f"{plid},31,{owner},{ins_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Life Estates / Future Interests (Type 25) ---
    le_cfg = cfg["life_estates"]
    # Type 25 = future interest in property (NOT beneficiary of a Will)
    le_val = _get(row, resolver, "You have a \"future interest\" in valuable property")
    le_val_spouse = ""
    if joint:
        le_val_spouse = _get(row, resolver, "You or your spouse have a \"future interest\" in valuable property")
    if le_val or le_val_spouse:
        plid = _next_link_id()
        if le_val_spouse and not le_val:
            desc = "You or your spouse have a future interest in valuable property"
        else:
            desc = "You have a future interest in valuable property"
        owner = special_owner
        lines.append(f"{plid},25,{owner},{le_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Decedent Property (Type 32) ---
    # Type 32 = beneficiary of a Will (separate from Type 25 future interests)
    dec_val = _get(row, resolver, "You are the beneficiary of a Will")
    dec_val_spouse = ""
    if joint:
        dec_val_spouse = _get(row, resolver, "You or your spouse are the beneficiary of a Will")
    if dec_val or dec_val_spouse:
        plid = _next_link_id()
        if dec_val_spouse and not dec_val:
            desc = "You or your spouse are the beneficiary of a Will"
        else:
            desc = "You are the beneficiary of a Will"
        owner = special_owner
        lines.append(f"{plid},32,{owner},0.00,,,{desc},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Intellectual Property (Type 26) ---
    ip_cfg = cfg["intellectual_property"]
    ip_items_ind = {
        "patent": "You own a Patent",
        "copyright": "You own a Copyright",
        "trademark": "You own a Trademark",
        "other_ip": "You own other Intellectual Property",
    }
    ip_items_jnt = {
        "patent": "You or your spouse own a Patent",
        "copyright": "You or your spouse own a Copyright",
        "trademark": "You or your spouse own a Trademark",
        "other_ip": "You or your spouse own other Intellectual Property",
    }
    for key, desc_text_ind in ip_items_ind.items():
        desc_text_jnt = ip_items_jnt[key]
        col_ref = ip_cfg["checkbox_cols"].get(key)
        ind_val = _get(row, resolver, col_ref) if col_ref else ""
        jnt_val = _get(row, resolver, desc_text_jnt) if joint else ""
        if ind_val or jnt_val:
            plid = _next_link_id()
            if jnt_val and not ind_val:
                desc_text = desc_text_jnt
            else:
                desc_text = desc_text_ind
            owner = special_owner
            lines.append(f"{plid},26,{owner},{ip_cfg['default_value']},,,Intellectual: {desc_text},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Child Support Owed TO debtor (Type 29) ---
    cs_cfg = cfg["child_support_owed"]
    cs_val = _get(row, resolver, cs_cfg["checkbox_col"])
    cs_val_spouse = ""
    if joint:
        cs_val_spouse = _get(row, resolver, "You or your spouse are owed past due or lump sum Alimony or Child Support")
    if cs_val or cs_val_spouse:
        plid = _next_link_id()
        if cs_val_spouse and not cs_val:
            desc = "Child Support: You or your spouse are owed past due or lump sum Alimony or Child Support"
        else:
            desc = "Child Support: You are owed past due or lump sum Alimony or Child Support"
        owner = special_owner
        lines.append(f"{plid},29,{owner},{cs_cfg['default_value']},,,{desc},,,,,,,{zero_flags},,{cs_cfg['default_addl_desc1']},,,,0,0,,")

    # --- Owed Other (Type 30) ---
    oo_cfg = cfg["owed_other"]
    notes = _get(row, resolver, oo_cfg["notes_col"])
    # For joint filing, also check unpaid wages checkbox from spouse set
    unpaid_wages_text = ""
    if joint:
        unpaid_wages_text = _get(row, resolver, "You or your spouse are owed Unpaid Wages, Disability Benefits, Workers\u2019 Compensation, or Social Security Benefits")
        if not unpaid_wages_text:
            unpaid_wages_text = _get(row, resolver, "You or your spouse are owed Unpaid Wages, Disability Benefits, Workers' Compensation, or Social Security Benefits")
    if unpaid_wages_text:
        plid = _next_link_id()
        desc = f"Owed Other: {unpaid_wages_text}"
        lines.append(f"{plid},30,{special_owner},{oo_cfg['default_value']},,,\"{desc}\",,,,,,,{zero_flags},,,,,,0,0,,")
    elif notes:
        plid = _next_link_id()
        notes_clean = notes.replace('"', '')
        desc = f"Owed Other: {notes_clean}"
        owner = special_owner
        lines.append(f"{plid},30,{owner},{oo_cfg['default_value']},,,\"{desc}\",,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Claims (Type 33) ---
    claims_col = cfg["claims"]["csv_col_expecting_payment"]
    has_lawsuits = _get(row, resolver, claims_col)
    # For joint filing, check the joint version too
    if not has_lawsuits and joint:
        has_lawsuits = _get(row, resolver, "In the last year, were you or your spouse part of any court case or legal matter?")
    # Check the explicit expecting-payment questions (old and new wording)
    if not has_lawsuits:
        has_lawsuits = _get(row, resolver, "Are you expecting to GET or RECEIVE money or payment from any lawsuits or claims?")
    if not has_lawsuits:
        has_lawsuits = _get(row, resolver, "Are you expecting to RECEIVE any money or payment from any lawsuits or claims?")
    if has_lawsuits and "Yes" in has_lawsuits:
        plid = _next_link_id()
        desc = "Claim: Yes, I expect future payment or compensation from a lawsuit or claim."
        owner = special_owner if joint else 1
        lines.append(f"{plid},33,{owner},1.00,,,\"{desc}\",,,,,,,{zero_flags},,,,,,0,0,,")
    elif has_lawsuits and "No" in has_lawsuits:
        plid = _next_link_id()
        desc = "No, I do not expect future payment or compensation from any lawsuits or claims."
        owner = special_owner if joint else 1
        lines.append(f"{plid},33,{owner},0.00,,,\"{desc}\",,,,,,,{zero_flags},,,,,,0,0,,")

    # --- General Intangibles (Type 27) ---
    gi_cfg = cfg["general_intangibles"]
    gi_items_ind = {
        "franchise": "You own a Franchise",
        "license": "You own a valuable License that can be sold or transferred",
    }
    gi_items_jnt = {
        "franchise": "You or your spouse own a Franchise",
        "license": "You or your spouse own a valuable License that can be sold or transferred",
    }
    for key, desc_text_ind in gi_items_ind.items():
        desc_text_jnt = gi_items_jnt.get(key)
        col_ref = gi_cfg["checkbox_cols"].get(key)
        ind_val = _get(row, resolver, col_ref) if col_ref else ""
        jnt_val = _get(row, resolver, desc_text_jnt) if joint and desc_text_jnt else ""
        if ind_val or jnt_val:
            plid = _next_link_id()
            if jnt_val and not ind_val:
                desc_text = desc_text_jnt
            else:
                desc_text = desc_text_ind
            owner = special_owner
            prefix = "General Intangibiles: " if key != "license" else ""
            value = "0.00" if key == "license" else gi_cfg['default_value']
            lines.append(f"{plid},27,{owner},{value},,,{prefix}{desc_text},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Vehicles (Type 3) ---
    v_cfg = cfg["vehicles"]
    for col_ref in v_cfg["csv_cols"]:
        veh = parse_vehicle(_get(row, resolver, col_ref))
        if not veh:
            continue
        plid = _next_link_id()
        lines.append(f"{plid},3,{veh_owner},{v_cfg['default_value']},,,Vehicle: Valuation Method: Edmunds.com Dealer Retail Value,,,,,,,{zero_flags},,{veh['make']},{veh['model']},{veh['year']},{veh['mileage']},0,0,,{veh['make']}")

    # --- Recreational Vehicles (Type 4) ---
    rv_cfg = cfg["recreational_vehicles"]
    for col_ref in rv_cfg["csv_cols"]:
        veh = parse_vehicle(_get(row, resolver, col_ref))
        if not veh:
            continue
        plid = _next_link_id()
        sub_type = veh.get("mileage", "")
        lines.append(f"{plid},4,{veh_owner},{rv_cfg['default_value']},,,Vehicle: {sub_type},,,,,,,{zero_flags},,{veh['make']},{veh['model']},{veh['year']},,0,0,,{veh['make']}")

    # --- Household Goods (Type 6) ---
    # For joint, check spouse checkbox set too
    hg_cfg = cfg["household_goods"]
    checked_items = []
    for col_ref in hg_cfg["debtor_checkbox_cols"]:
        val = _get(row, resolver, col_ref)
        if val:
            checked_items.append(val)
    # Check spouse HHG checkboxes if no debtor items (joint filing)
    if not checked_items and joint:
        for col_ref in hg_cfg["debtor_checkbox_cols"]:
            val = _get(row, resolver, f"{col_ref}#2")
            if val:
                checked_items.append(val)
    addl_notes = _get(row, resolver, hg_cfg["additional_notes_col"], "")
    if not addl_notes and joint:
        addl_notes = _get(row, resolver, f"{hg_cfg['additional_notes_col']}#2", "")
    if checked_items:
        plid = _next_link_id()
        all_items = list(checked_items)
        if addl_notes:
            all_items.append(addl_notes)
        # Include "Anything Else?" personal property notes for joint filing
        if joint:
            pp_notes = _get(row, resolver, "Anything Else?#2", "")
            if pp_notes:
                all_items.append(pp_notes)
        item_str = "; ".join(all_items)
        desc = item_str
        lines.append(f"{plid},6,{hhg_owner},{hg_cfg['default_value']},,,{_csv_field(desc)},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Electronics (Type 7) ---
    el_cfg = cfg["electronics"]
    checked_items = []
    for col_ref in el_cfg["debtor_checkbox_cols"]:
        val = _get(row, resolver, col_ref)
        if val:
            checked_items.append(val)
    # Check spouse electronics if no debtor items (joint filing)
    # Use electronics checkbox cols #2 (NOT HHG cols)
    if not checked_items and joint:
        for col_ref in el_cfg["debtor_checkbox_cols"]:
            val = _get(row, resolver, f"{col_ref}#2")
            if val:
                checked_items.append(val)
    # Additional notes
    el_hhg_notes = ""
    el_notes = ""
    if joint:
        el_hhg_notes = _get(row, resolver, f"{hg_cfg['additional_notes_col']}#2", "")
        el_notes = _get(row, resolver, f"{el_cfg['additional_notes_col']}#2", "")
    else:
        el_notes = _get(row, resolver, el_cfg["additional_notes_col"], "")
    if checked_items:
        plid = _next_link_id()
        all_items = list(checked_items)
        if el_hhg_notes:
            all_items.append(el_hhg_notes)
        if el_notes:
            all_items.append(el_notes)
        item_str = "; ".join(all_items)
        desc = item_str
        lines.append(f"{plid},7,{hhg_owner},{el_cfg['default_value']},,,{_csv_field(desc)},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Clothes (Type 11) - only for individual filings ---
    if not joint:
        plid = _next_link_id()
        lines.append(f"{plid},11,1,{cfg['clothes']['default_value']},,,Used clothing. Value only to debtor.,,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Collectibles (Type 8) ---
    coll_cfg = cfg["collectibles"]
    val = _get(row, resolver, coll_cfg["csv_col"])
    if not val and joint:
        val = _get(row, resolver, f"{coll_cfg['csv_col']}#2")
    if val:
        plid = _next_link_id()
        lines.append(f"{plid},8,{hhg_owner},{coll_cfg['default_value']},,,{val},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Sports/Hobby (Type 9) ---
    sp_cfg = cfg["sports_hobby"]
    val = _get(row, resolver, sp_cfg["csv_col"])
    if not val and joint:
        val = _get(row, resolver, f"{sp_cfg['csv_col']}#2")
    if val:
        plid = _next_link_id()
        lines.append(f"{plid},9,{hhg_owner},{sp_cfg['default_value']},,,{val},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Firearms (Type 10) ---
    fa_cfg = cfg["firearms"]
    val = _get(row, resolver, fa_cfg["csv_col"])
    if not val and joint:
        val = _get(row, resolver, f"{fa_cfg['csv_col']}#2")
    if val:
        plid = _next_link_id()
        desc = _normalize_inline_whitespace(val)
        lines.append(f"{plid},10,{hhg_owner},{fa_cfg['default_value']},,,{_csv_field(desc)},,,,,,,{zero_flags},,,,,,0,0,,")

    # --- Other Personal Property (Type 53) ---
    op_cfg = cfg["other_personal_property"]
    notes = _get(row, resolver, op_cfg["notes_col"])
    if not notes and joint:
        notes = _get(row, resolver, "Anything Else?#4")
    if notes:
        plid = _next_link_id()
        notes_clean = notes.replace('"', '')
        desc = notes_clean
        state_abbrev = state_to_abbrev(_get(row, resolver, op_cfg.get("debtor_state_col", "Your Current Address (State / Province)"), ""))
        owner = hhg_owner if joint else 1
        lines.append(f"{plid},53,{owner},{op_cfg['default_value']},,,{_csv_field(desc)},,,,,{state_abbrev},,{zero_flags},,,,,,0,0,,")

    return lines


# ---------------------------------------------------------------------------
# [SchDEF] section - Creditors (Schedules D, E, F)
# ---------------------------------------------------------------------------

_clink_counter = 3291288

def _next_clink_id():
    global _clink_counter
    _clink_counter += 1
    return _clink_counter

def _reset_clink_counter(start=3291287):
    global _clink_counter
    _clink_counter = start


def build_schdef_section(row, mapping, resolver):
    cfg = mapping["schedule_def"]
    header = "CLinkID,Owner,Schedule,Name,Address1,Address2,Address3,City,State,Zip,Country,AccountNo,MaskAcct,DateIncurred,Contingent,Unliquidated,Disputed,Setoff,ClaimAmt,Amended,ExSchedule,ExMatrix,PropertyName,PLinkID,LienNature,PropertyValue,UnsecuredAmt,SeniorLiens,Consideration,PriorityAmt,CreditorType,LNAgreement,LNJudgement,LNStatutory,LNOther,PriorityType"
    lines = [header]

    # Known tax authority addresses (used by both non-consumer and tax debt sections)
    _known_tax_addresses = {
        "irs": {"addr": "PO Box 7346", "city": "Philadelphia", "state": "PA", "zip": "19101-7346"},
        "internal revenue service": {"addr": "PO Box 7346", "city": "Philadelphia", "state": "PA", "zip": "19101-7346"},
    }
    _tax_authority_keywords = {"irs", "internal revenue service", "internal revenue"}

    # Non-consumer debt (Schedule F), but IRS/tax authority names → Schedule E
    nc_cfg = cfg["non_consumer_debt"]
    # Dynamically discover all Non-Consumer Debt Info columns
    nc_cols = list(nc_cfg["csv_cols"])
    n = len(nc_cols) + 1
    while True:
        extra = f"Non-Consumer Debt Info {n}"
        if resolver.resolve(extra) is None:
            break
        nc_cols.append(extra)
        n += 1
    for col_ref in nc_cols:
        cred = parse_creditor(_get(row, resolver, col_ref))
        if not cred:
            continue
        clid = _next_clink_id()
        name = _csv_field(cred['name'])
        name_lower = cred['name'].lower()
        # If creditor name is IRS/Internal Revenue, treat as tax debt (Schedule E, priority type 7)
        if any(kw in name_lower for kw in _tax_authority_keywords):
            known = _known_tax_addresses.get("irs")
            lines.append(
                f"{clid},3,E,{name},{known['addr']},,,{known['city']},{known['state']},{known['zip']},"
                f",,1,,0,,0,,{format_amount(cred['amount'])},0,0,0,,0,Taxes and certain other debts,,,,"
                f",0,7,,,,,7"
            )
        else:
            addr = _csv_field(cred['address'])
            cred_state = state_to_abbrev(cred['state'])
            lines.append(
                f"{clid},3,F,{name},{addr},,,{cred['city']},{cred_state},{cred['zip']},"
                f",,1,,0,,0,,{format_amount(cred['amount'])},0,0,0,,0,{nc_cfg['creditor_type_default']},,,,,0,1,,,,"
            )

    # Child support debt (Schedule E)
    cs_cfg = cfg["child_support_debt"]
    recipient = parse_support_recipient(_get(row, resolver, cs_cfg["csv_col_recipient"]))
    cs_amount = _get(row, resolver, cs_cfg["csv_col_amount"])
    if recipient and cs_amount:
        clid = _next_clink_id()
        lines.append(
            f"{clid},3,E,{recipient['name']},{recipient['address']},,,{recipient['city']},{recipient['state']},{recipient['zip']},"
            f",,1,,0,,0,,{format_amount(cs_amount)},0,0,0,,0,Support,,,,Past Due Domestic Support Obligation,0,1,,,,,1"
        )

    # Tax debt (Schedule E)
    tax_cfg = cfg["tax_debt"]
    tax_cols = list(tax_cfg["csv_cols"])
    n = len(tax_cols) + 1
    while True:
        extra = f"Tax Debt {n}"
        if resolver.resolve(extra) is None:
            break
        tax_cols.append(extra)
        n += 1

    for col_ref in tax_cols:
        td = parse_tax_debt(_get(row, resolver, col_ref))
        if not td:
            continue
        clid = _next_clink_id()
        entity = td['entity']
        entity_lower = entity.lower()
        # IRS gets a real address; for non-IRS entities keep address blank unless
        # we have explicit source values.
        if entity_lower in ("irs", "internal revenue service"):
            tax_addr = "PO Box 7346"
            tax_city = "Philadelphia"
            tax_state = "PA"
            tax_zip = "19101-7346"
        else:
            tax_addr = ""
            tax_city = ""
            tax_state = ""
            tax_zip = ""
        # Quote tax years if they contain commas
        years = td['years']
        if ',' in years:
            years = f'"{years}"'
        lines.append(
            f"{clid},3,E,{entity},{tax_addr},,,{tax_city},{tax_state},{tax_zip},"
            f",,1,,0,,0,,{format_amount(td['amount'])},0,0,0,,0,Taxes and certain other debts,,,,"
            f"{years},0,7,,,,,7"
        )

    return lines


# ---------------------------------------------------------------------------
# Empty sections (stubs)
# ---------------------------------------------------------------------------

def build_empty_section():
    return []


# ---------------------------------------------------------------------------
# [SFA] section - Statement of Financial Affairs
# ---------------------------------------------------------------------------

def build_sfa_section(row, mapping, resolver):
    cfg = mapping["sfa"]
    now = datetime.now()
    joint = _is_joint(row, resolver)
    lines = []

    # Line 1: Current marital status (0 = not married, 1 = married/separated)
    marital_flag = "1" if _is_married(row, resolver) else "0"
    lines.append(f"1,{marital_flag}")

    # Line 2: Previous addresses
    pa = cfg["previous_address"]
    addr = _get(row, resolver, pa["addr_col"], "")
    addr2 = _get(row, resolver, pa.get("addr2_col"), "")
    city = _get(row, resolver, pa["city_col"], "")
    state = state_to_abbrev(_get(row, resolver, pa["state_col"], ""))
    zipcode = _get(row, resolver, pa["zip_col"], "")
    dates = _get(row, resolver, pa["dates_col"], "")
    if addr:
        full_addr = f"{addr}|" if not addr2 else f"{addr}|{addr2}"
        # Strip zip to just 5 digits for SFA
        zip5 = zipcode.split()[0].split("-")[0] if zipcode else ""
        # Joint filing: last two flags are 1,1 instead of 0,0
        sfa2_flags = "1,1" if joint else "0,0"
        lines.append(f"2,,{full_addr},{city},{state},{zip5},,,,,,{dates},,{sfa2_flags}")

    # Line 4: Income from employment or business
    # Format: 4,Year,D1_has,D1_flag,D1_amount,D2_has,D2_flag,D2_amount
    # Entry 1 = Wages row, Entry 2 = Business row (positional in Best Case)
    year = now.year
    debtor_biz = _get(row, resolver, "What's the name of your business?", "")
    debtor_biz_income = _get(row, resolver, "Average monthly income from operation of this business", "")

    if joint:
        spouse_biz = _get(row, resolver, "What's the name of your spouse's business?", "")
        spouse_biz_income = _get(row, resolver, "Average monthly income from operation of your spouse's business", "")

        d1_has_biz = 1 if debtor_biz else 0
        d1_biz_amt = format_amount(debtor_biz_income) if debtor_biz_income else "0"
        d2_has_biz = 1 if spouse_biz else 0
        d2_biz_amt = format_amount(spouse_biz_income) if spouse_biz_income else "0"

        if d1_has_biz or d2_has_biz:
            # Wages row: no wages (user wants wages unselected)
            lines.append(f"4,{year},0,0,0,0,0,0")
            # Business row
            lines.append(f"4,{year},{d1_has_biz},0,{d1_biz_amt},{d2_has_biz},0,{d2_biz_amt}")
    else:
        if debtor_biz:
            biz_amt = format_amount(debtor_biz_income) if debtor_biz_income else "0"
            # Wages row: no wages
            lines.append(f"4,{year},0,0,0,0,0,0")
            # Business row: D1 only
            lines.append(f"4,{year},1,0,{biz_amt},0,0,0")

    # Line 5: Other income (VA Benefits, rental, alimony, etc.)
    oi = cfg.get("other_income", mapping["income"]["other_income"])
    va_benefits = _get(row, resolver, oi.get("va_benefits_col", "Your average monthly income from VA Benefits"), "")
    other1 = _get(row, resolver, oi.get("other1_col", "Average monthly income from other sources 1"), "")
    if va_benefits:
        lines.append(f"5,{year},VA Benefits,{format_amount(va_benefits)},,0")
    if other1:
        lines.append(f"5,{year},{other1},,0")

    # Line 7: Insider loan repayments (no fake address)
    il_cfg = cfg["insider_loans"]
    for col_ref in il_cfg["csv_cols"]:
        loan = parse_insider_loan(_get(row, resolver, col_ref))
        if not loan:
            continue
        lines.append(
            f"7,{loan['name']},,,,"
            f",{loan['date']},{format_amount(loan['amount'])},0.00,{loan['description']}"
        )

    # Line 9: Lawsuits / Court cases
    law_cfg = cfg.get("lawsuits", {})
    case_type_col = law_cfg.get("case_type_col", "What type of case or claim is it?")
    for occurrence in range(1, 11):
        suffix = "" if occurrence == 1 else f"#{occurrence}"

        active_type = _get(row, resolver, f"{case_type_col}{suffix}")
        if not active_type:
            continue

        location_ref = f"{law_cfg['location_col']}{suffix}" if occurrence > 1 else law_cfg['location_col']
        status_ref = f"{law_cfg['status_col']}{suffix}" if occurrence > 1 else law_cfg['status_col']
        has_num_ref = f"{law_cfg['has_number_col']}{suffix}" if occurrence > 1 else law_cfg['has_number_col']
        case_num_ref = f"{law_cfg['case_number_col']}{suffix}" if occurrence > 1 else law_cfg['case_number_col']

        location = _get(row, resolver, location_ref, "")
        status = _get(row, resolver, status_ref, "")
        has_num = _get(row, resolver, has_num_ref, "")
        case_num = _get(row, resolver, case_num_ref, "") if has_num == "Yes" else ""

        # Skip entries where has_number=Yes but no actual case number
        if has_num == "Yes" and not case_num:
            more_ref = f"{law_cfg['more_col']}{suffix}" if occurrence > 1 else law_cfg['more_col']
            more = _get(row, resolver, more_ref, "")
            if more != "Yes":
                break
            continue

        lawsuit = parse_lawsuit(active_type, location, status, has_num, case_num)
        if lawsuit:
            county = lawsuit["county"]
            state_part = lawsuit.get("state", "")
            # Format court name as "County County, State" (full state name)
            if county and state_part:
                location_field = f"{county} County, {state_part}"
            elif county:
                location_field = county
            else:
                location_field = state_part
            # No fake courthouse address
            lines.append(
                f"9,,{case_num},{active_type},{location_field},,,,{lawsuit['status']},"
            )

        # Check if there are more
        more_ref = f"{law_cfg['more_col']}{suffix}" if occurrence > 1 else law_cfg['more_col']
        more = _get(row, resolver, more_ref, "")
        if more != "Yes":
            break

    # Line 10: Seizures (Repossession, Foreclosure, Garnishment, Levy) - no fake addresses
    sz_cfg = cfg["seizures"]
    for idx, col_ref in enumerate(sz_cfg["repossession_cols"]):
        sz = parse_seizure(_get(row, resolver, col_ref))
        if not sz:
            continue
        dt = format_date_leading_zeros(sz['date'])
        lines.append(f"10,{sz['name']},,,,,{sz['description']},0,{dt},1,0,0,0")

    for idx, col_ref in enumerate(sz_cfg["foreclosure_cols"]):
        sz = parse_seizure(_get(row, resolver, col_ref))
        if not sz:
            continue
        dt = format_date_leading_zeros(sz['date'])
        lines.append(f"10,{sz['name']},,,,,{sz['description']},0,{dt},0,1,0,0")

    for idx, col_ref in enumerate(sz_cfg["garnishment_cols"]):
        sz = parse_seizure(_get(row, resolver, col_ref))
        if not sz:
            continue
        dt = format_date_leading_zeros(sz['date'])
        lines.append(f"10,{sz['name']},,,,,,0,{dt},0,0,1,0")

    for idx, col_ref in enumerate(sz_cfg["levy_cols"]):
        sz = parse_seizure(_get(row, resolver, col_ref))
        if not sz:
            continue
        dt = format_date_leading_zeros(sz['date'])
        lines.append(f"10,{sz['name']},,,,,{sz['description']},0,{dt},0,0,0,1")

    # Line 12: Settlements (currently just a flag)
    lines.append("12,0")

    # Line 13: Gifts (no fake address)
    g_cfg = cfg["gifts"]
    for idx, col_ref in enumerate(g_cfg["csv_cols"]):
        gift = parse_gift(_get(row, resolver, col_ref))
        if not gift:
            continue
        dt = format_date_leading_zeros(gift['date'])
        lines.append(f"13,{gift['name']},,,,,,{dt},{gift['description']},{gift['amount']}")

    # Line 15: Losses
    l_cfg = cfg["losses"]
    for col_ref in l_cfg["csv_cols"]:
        loss = parse_loss(_get(row, resolver, col_ref))
        if not loss:
            continue
        dt = format_date_leading_zeros(loss['date'])
        lines.append(f"15,{loss['description']},,{dt},{loss['amount']}")

    # Line 18: Transfers of property
    t_cfg = cfg["transfers"]
    recipient = _get(row, resolver, t_cfg["recipient_col"], "")
    relationship = _get(row, resolver, t_cfg["relationship_col"], "")
    what_transferred = _get(row, resolver, t_cfg["what_col"], "")
    transfer_value = _get(row, resolver, t_cfg["value_col"], "")
    received = _get(row, resolver, t_cfg["received_col"], "")
    transfer_date = _get(row, resolver, t_cfg["date_col"], "")
    if recipient:
        dt = format_date_leading_zeros(transfer_date)
        lines.append(
            f"18,{recipient},,,,"
            f",{relationship},{dt},{what_transferred},"
            f"\"{received}|Value: {transfer_value}\""
        )

    # Line 20: Closed/transferred accounts (no fake address, with account type)
    ca_cfg = cfg["closed_accounts"]
    # Account type dropdown in field 7 (numeric): 0=Checking, 1=Savings, 2=Money Market, 3=Brokerage
    # Field 8 = "Other account type details" text (only used if type is Other/unknown)
    _acct_type_map = {"checking": "0", "savings": "1", "money market": "2", "brokerage": "3"}
    for idx, col_ref in enumerate(ca_cfg["csv_cols"]):
        acct = parse_closed_account(_get(row, resolver, col_ref))
        if not acct:
            continue
        dt = format_date_leading_zeros(acct['date'])
        acct_lower = acct["type"].lower()
        type_code = _acct_type_map.get(acct_lower, "")
        other_detail = acct["type"] if acct_lower not in _acct_type_map else ""
        lines.append(
            f"20,{acct['name']},,,,{type_code},{other_detail},{dt},,{format_amount(acct['balance'])}"
        )

    # Line 21: Safe deposit boxes (no fake address)
    sd_cfg = cfg["safe_deposit"]
    for idx, col_ref in enumerate(sd_cfg["csv_cols"]):
        raw = _get(row, resolver, col_ref)
        if not raw:
            continue
        parts = parse_pipe_list(raw)
        bank = parts[0] if len(parts) > 0 else ""
        contents = parts[1] if len(parts) > 1 else ""
        lines.append(
            f"21,{bank},,,,"
            f",,{contents},1"
        )

    # Line 22: Storage units (no fake address)
    su_cfg = cfg["storage_units"]
    for idx, col_ref in enumerate(su_cfg["csv_cols"]):
        raw = _get(row, resolver, col_ref)
        if not raw:
            continue
        parts = parse_pipe_list(raw)
        location = parts[0] if len(parts) > 0 else ""
        contents = parts[1] if len(parts) > 1 else ""
        lines.append(
            f"22,{location},,,,"
            f",,{contents},1"
        )

    # Line 27: Business interests (debtor + spouse businesses)
    # Debtor business (if self-employed)
    debtor_biz_name_27 = _get(row, resolver, "What's the name of your business?")
    debtor_biz_nature_27 = _get(row, resolver, "What's the nature of your business?")
    debtor_biz_nature_27 = _normalize_multi_value_text(debtor_biz_nature_27)
    if debtor_biz_name_27:
        dcfg = mapping["debtor"]["fields"]
        home_addr = _get(row, resolver, dcfg["Addr"]["csv_col"])
        home_city = _get(row, resolver, dcfg["City"]["csv_col"])
        home_state = state_to_abbrev(_get(row, resolver, dcfg["State"]["csv_col"]))
        home_zip = _get(row, resolver, dcfg["Zip"]["csv_col"])
        home_zip5 = home_zip.split()[0].split("-")[0] if home_zip else ""
        biz_same = _get(row, resolver, "Is the business address the same as your home address?")
        if biz_same == "No":
            b_addr = _get(row, resolver, "Address of the Business (Street Address)")
            b_city = _get(row, resolver, "Address of the Business (City)")
            b_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)"))
            b_zip = _get(row, resolver, "Address of the Business (ZIP / Postal Code)")
        else:
            b_addr, b_city, b_state, b_zip = home_addr, home_city, home_state, home_zip5
        biz_name_q = f'"{debtor_biz_name_27}"' if ',' in debtor_biz_name_27 else debtor_biz_name_27
        lines.append(f"27,{biz_name_q},{b_addr}  ,{b_city},{b_state},{b_zip},{_csv_field(debtor_biz_nature_27)},,,")

    if joint:
        dcfg = mapping["debtor"]["fields"]
        home_addr = _get(row, resolver, dcfg["Addr"]["csv_col"])
        home_city = _get(row, resolver, dcfg["City"]["csv_col"])
        home_state = state_to_abbrev(_get(row, resolver, dcfg["State"]["csv_col"]))
        home_zip = _get(row, resolver, dcfg["Zip"]["csv_col"])
        home_zip5 = home_zip.split()[0].split("-")[0] if home_zip else ""

        # Spouse business 1 - uses #3 occurrence of address cols (after debtor #1 and debtor addl #2)
        biz1_name = _get(row, resolver, "What's the name of your spouse's business?")
        biz1_nature = _get(row, resolver, "What's the nature of your spouse's business?")
        biz1_nature = _normalize_multi_value_text(biz1_nature)
        if biz1_name:
            biz1_same = _get(row, resolver, "Is the business address the same as your home address?#3")
            if biz1_same == "No":
                b_addr = _get(row, resolver, "Address of the Business (Street Address)#3")
                b_city = _get(row, resolver, "Address of the Business (City)#3")
                b_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)#3"))
                b_zip = _get(row, resolver, "Address of the Business (ZIP / Postal Code)#3")
            else:
                b_addr, b_city, b_state, b_zip = home_addr, home_city, home_state, home_zip5
            biz1_name_q = f'"{biz1_name}"' if ',' in biz1_name else biz1_name
            lines.append(f"27,{biz1_name_q},{b_addr}  ,{b_city},{b_state},{b_zip},{_csv_field(biz1_nature)},,,")

        # Spouse additional business - uses #4 occurrence
        biz2_name = _get(row, resolver, "What's the name of this additional business?#2")
        biz2_nature = _get(row, resolver, "What's the nature of this additional business?#2")
        if not biz2_name:
            biz2_name = _get(row, resolver, "What's the name of this additional business?")
            biz2_nature = _get(row, resolver, "What's the nature of this additional business?")
        biz2_nature = _normalize_multi_value_text(biz2_nature)
        if biz2_name:
            biz2_same = _get(row, resolver, "Is the business address the same as your home address?#4")
            if biz2_same == "Yes" or not _get(row, resolver, "Address of the Business (Street Address)#4"):
                b_addr, b_city, b_state, b_zip = home_addr, home_city, home_state, home_zip5
            else:
                b_addr = _get(row, resolver, "Address of the Business (Street Address)#4")
                b_city = _get(row, resolver, "Address of the Business (City)#4")
                b_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)#4"))
                b_zip = _get(row, resolver, "Address of the Business (ZIP / Postal Code)#4")
            biz2_name_q = f'"{biz2_name}"' if ',' in biz2_name else biz2_name
            lines.append(f"27,{biz2_name_q},{b_addr}  ,{b_city},{b_state},{b_zip},{_csv_field(biz2_nature)},,,")

        # End-of-questionnaire business (Business/Corporate Interests section)
        eoq_name = _get(row, resolver, "Name of the Business")
        eoq_nature = _get(row, resolver, "Nature of the Business")
        eoq_nature = _normalize_multi_value_text(eoq_nature)
        if eoq_name:
            eoq_same = _get(row, resolver, "Is the business address the same as your home address?#5")
            if not eoq_same:
                eoq_same = "Yes"
            if eoq_same == "Yes" or not _get(row, resolver, "Address of the Business (Street Address)#5"):
                b_addr, b_city, b_state, b_zip = home_addr, home_city, home_state, home_zip5
            else:
                b_addr = _get(row, resolver, "Address of the Business (Street Address)#5")
                b_city = _get(row, resolver, "Address of the Business (City)#5")
                b_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)#5"))
                b_zip = _get(row, resolver, "Address of the Business (ZIP / Postal Code)#5")
            eoq_name_q = f'"{eoq_name}"' if ',' in eoq_name else eoq_name
            lines.append(f"27,{eoq_name_q},{b_addr}  ,{b_city},{b_state},{b_zip},{_csv_field(eoq_nature)},,,")

    # Line 28: Mortgage payoff / divorce only (no unconditional placeholder rows)

    # Mortgage Payoff or Refinance
    mortgage_payoff = _get(row, resolver, "Mortgage Payoff or Refinance")
    if mortgage_payoff == "Yes":
        mortgage_desc = _get(row, resolver, "Please describe each payoff or refinance below.")
        lines.append(f"28,*** MORTGAGE PAYOFF OR REFINANCE ***,,,,,{mortgage_desc},,,")

    # Recent Divorce (check both debtor and joint columns)
    recent_divorce = _get(row, resolver, "Recent Divorce")
    if not recent_divorce and joint:
        recent_divorce = _get(row, resolver, "Recent Divorce#2")
    if recent_divorce == "Yes":
        divorce_date = _get(row, resolver, "Please provide the date of the divorce filing or date the Marital Settlement Agreement (MSA) was finalized.")
        lines.append(f"28,*** RECENT DIVORCE ***,,,,,{divorce_date},,,")

    return lines


# ---------------------------------------------------------------------------
# [Income] section
# ---------------------------------------------------------------------------

def build_income_section(row, mapping, resolver):
    cfg = mapping["income"]
    joint = _is_joint(row, resolver)
    married = _is_married(row, resolver)
    lines = []

    # Marital status: set explicitly for married-but-individual filers
    # Joint filers: BestCase sets Married automatically
    # Individual filers who answered the joint/individual question: married, set Married
    # Single filers: no answer to that question, leave blank
    if not joint:
        filing_q = _get(row, resolver, "Do you plan on filing your case individually or jointly with your spouse?", "")
        if filing_q:
            lines.append("MaritalStat=Married")

    # Debtor employment - detect self-employed vs W2
    d = cfg["debtor"]
    w2_employer = _get(row, resolver, d["employer_col"], "")
    w2_occupation = _get(row, resolver, d["occupation_col"], "")
    biz_name_d = _get(row, resolver, "What's the name of your business?", "")
    biz_nature_d = _get(row, resolver, "What's the nature of your business?", "")

    # If no W2 employer but has business, use business fields (self-employed)
    if not w2_employer and biz_name_d:
        employer = biz_name_d
        occupation = biz_nature_d
        how_long_raw = _get(row, resolver, "Approximately how long have you been self-employed? 1", "")
    else:
        employer = w2_employer
        occupation = w2_occupation
        how_long_raw = _get(row, resolver, d["how_long_col"], "")

    hl_parts = parse_pipe_list(how_long_raw)
    years = hl_parts[0] if len(hl_parts) > 0 else "0"
    months = hl_parts[1] if len(hl_parts) > 1 else "0"
    how_long_str = f"{years} Years, {months} Months"

    lines.append(f"dOccupation={occupation}")
    lines.append(f"dEmployer={employer}")

    # Debtor employer address
    dcfg = mapping["debtor"]["fields"]
    if not w2_employer and biz_name_d:
        # Self-employed: use business address or home address
        biz_same = _get(row, resolver, "Is the business address the same as your home address?")
        if biz_same == "Yes" or biz_same == "":
            d_addr = _get(row, resolver, dcfg["Addr"]["csv_col"])
            d_city = _get(row, resolver, dcfg["City"]["csv_col"])
            d_state = state_to_abbrev(_get(row, resolver, dcfg["State"]["csv_col"]))
            d_zip = _get(row, resolver, dcfg["Zip"]["csv_col"])
            d_zip5 = d_zip.split()[0].split("-")[0] if d_zip else ""
        else:
            d_addr = _get(row, resolver, "Address of the Business (Street Address)")
            d_city = _get(row, resolver, "Address of the Business (City)")
            d_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)"))
            d_zip5 = _get(row, resolver, "Address of the Business (ZIP / Postal Code)")
        lines.append(f"dEmpAddr={d_addr}||")
        lines.append(f"dEmpCity={d_city}")
        lines.append(f"dEmpState={d_state}")
        lines.append(f"dEmpZip={d_zip5}-00000")
    elif w2_employer:
        # W2 employer: keep address fields blank when no source address is provided.
        lines.append(f"dEmpAddr=||")
        lines.append(f"dEmpCity=")
        lines.append(f"dEmpState=")
        lines.append(f"dEmpZip=")
    else:
        lines.append(f"dEmpAddr=||")
        lines.append(f"dEmpCity=")
        lines.append(f"dEmpState=")
        lines.append(f"dEmpZip=")

    lines.append(f"dHowLong={how_long_str}")

    # Pay period: 5=Monthly for both W2 and self-employed
    if not w2_employer and biz_name_d:
        lines.append(f"dPayPeriod=5")
    else:
        lines.append(f"dPayPeriod=5")

    # Debtor business income
    debtor_biz_income = _get(row, resolver, "Average monthly income from operation of this business", "")
    if not w2_employer and biz_name_d and debtor_biz_income:
        lines.append(f"dBusiness={format_amount(debtor_biz_income)}")
        lines.append(f"dBizExpense=1.00")
        lines.append(f"dBizNet={format_amount(debtor_biz_income)}")
    elif w2_employer and debtor_biz_income:
        # Some questionnaires include both W2 employment and side-business income.
        # Preserve the business gross amount for Schedule I "Other Income" parity.
        lines.append(f"dBusiness={format_amount(debtor_biz_income)}")

    if not joint:
        # Individual: include wages and other income line items
        oi = cfg["other_income"]
        pension = _get(row, resolver, oi["pension_col"], "")
        rental = _get(row, resolver, oi["rental_col"], "")
        alimony = _get(row, resolver, oi["alimony_col"], "")
        ss = _get(row, resolver, oi["social_security_col"], "")
        unemployment = _get(row, resolver, oi["unemployment_col"], "")

        lines.append(f"dPension={format_amount(pension)}")
        lines.append(f"dProperty={format_amount(rental)}")
        lines.append(f"dAlimony={format_amount(alimony)}")
        lines.append(f"dSocSecurity={format_amount(ss)}")
        lines.append(f"dUnemployment={format_amount(unemployment)}")

    lines.append(f"ExpChangeYES=0")

    if joint:
        # Joint: ExpChange references spouse's employer/business or debtor's employer
        spouse_biz = _get(row, resolver, "What's the name of your spouse's business?")
        spouse_w2 = _get(row, resolver, "Your Spouse's Current Employer")
        exp_change_name = spouse_biz or spouse_w2 or employer
    else:
        exp_change_name = employer
    if exp_change_name:
        lines.append(f"ExpChange={exp_change_name} Change: |")
    else:
        lines.append(f"ExpChange=")
    lines.append("")

    if joint or married:
        # Determine if spouse is W2 or self-employed
        spouse_w2_employer = _get(row, resolver, "Your Spouse's Current Employer", "")
        spouse_biz = _get(row, resolver, "What's the name of your spouse's business?")

        if spouse_w2_employer:
            # Spouse is W2 employee - do NOT populate address (per Mapping Helper ROWs 45-48)
            spouse_occupation = _get(row, resolver, "Your Spouse's Current Occupation", "")
            spouse_how_long_raw = _get(row, resolver, "Approximately how long has your spouse been employed here? 1", "")
            s_hl = parse_pipe_list(spouse_how_long_raw)
            s_years = s_hl[0] if len(s_hl) > 0 else "0"
            s_months = s_hl[1] if len(s_hl) > 1 else "0"

            lines.append(f"sOccupation={spouse_occupation}")
            lines.append(f"sEmployer={spouse_w2_employer}")

            # Spouse W2 employer: leave address empty
            lines.append(f"sEmpAddr=||")
            lines.append(f"sEmpCity=")
            lines.append(f"sEmpState=")
            lines.append(f"sEmpZip=")
            lines.append(f"sHowLong={s_years} Years, {s_months} Months")
            lines.append(f"sPayPeriod=5")  # Monthly

            # Spouse rental income (even if W2 employed)
            spouse_rental = _get(row, resolver, "Your spouse's average monthly income from Rental Property", "")
            if spouse_rental:
                lines.append(f"sProperty={format_amount(spouse_rental)}")
        elif spouse_biz:
            # Spouse is self-employed
            spouse_nature = _get(row, resolver, "What's the nature of your spouse's business?")
            spouse_how_long_raw = _get(row, resolver, "Approximately how long has your spouse been self-employed? 1")
            s_hl = parse_pipe_list(spouse_how_long_raw)
            s_years = s_hl[0] if len(s_hl) > 0 else "0"
            s_months = s_hl[1] if len(s_hl) > 1 else "0"

            lines.append(f"sOccupation={spouse_nature or ''}")
            lines.append(f"sEmployer={spouse_biz or ''}")

            # Spouse employer address: use spouse's business address (#3 occurrence)
            biz_same = _get(row, resolver, "Is the business address the same as your home address?#3")
            if biz_same == "No":
                s_addr = _get(row, resolver, "Address of the Business (Street Address)#3")
                s_city = _get(row, resolver, "Address of the Business (City)#3")
                s_state = state_to_abbrev(_get(row, resolver, "Address of the Business (State / Province)#3"))
                s_zip = _get(row, resolver, "Address of the Business (ZIP / Postal Code)#3")
            else:
                dcfg = mapping["debtor"]["fields"]
                s_addr = _get(row, resolver, dcfg["Addr"]["csv_col"])
                s_city = _get(row, resolver, dcfg["City"]["csv_col"])
                s_state = state_to_abbrev(_get(row, resolver, dcfg["State"]["csv_col"]))
                s_zip = _get(row, resolver, dcfg["Zip"]["csv_col"])
                s_zip = s_zip.split()[0].split("-")[0] if s_zip else ""

            lines.append(f"sEmpAddr={s_addr}||")
            lines.append(f"sEmpCity={s_city}")
            lines.append(f"sEmpState={s_state}")
            lines.append(f"sEmpZip={s_zip}-00000")
            lines.append(f"sHowLong={s_years} Years, {s_months} Months")
            lines.append(f"sPayPeriod=5")

            # Spouse rental income (gross income from real property)
            spouse_rental = _get(row, resolver, "Your spouse's average monthly income from Rental Property", "")
            if spouse_rental:
                lines.append(f"sProperty={format_amount(spouse_rental)}")

            # Spouse business income (both primary and additional businesses)
            spouse_biz_income = _get(row, resolver, "Average monthly income from operation of your spouse's business")
            spouse_biz2_income = _get(row, resolver, "Average monthly income from operation of this additional business#2")
            if not spouse_biz2_income:
                spouse_biz2_income = _get(row, resolver, "Average monthly income from operation of this additional business")
            total_biz = 0.0
            if spouse_biz_income:
                try:
                    total_biz += float(spouse_biz_income)
                except ValueError:
                    pass
            if spouse_biz2_income:
                try:
                    total_biz += float(spouse_biz2_income)
                except ValueError:
                    pass
            biz_total_str = format_amount(str(total_biz)) if total_biz else "0.00"
            lines.append(f"sBusiness={biz_total_str}")
            lines.append(f"sBusiness={biz_total_str}")

            # Business expenses (placeholder)
            lines.append(f"sBizExpense=2.00")
            lines.append(f"sBizNet={format_amount(str(total_biz - 2.0))}")
        else:
            # Spouse not employed
            lines.append(f"sEmpAddr=||")
            lines.append(f"sPayPeriod=0")
            lines.append(f"sNotEmployed=1")
    else:
        # Single filer: spouse not employed
        lines.append(f"sEmpAddr=||")
        lines.append(f"sPayPeriod=0")
        lines.append(f"sNotEmployed=1")
    lines.append("")
    lines.append("")

    # Other income descriptions
    oi = cfg["other_income"]
    other1 = parse_other_income(_get(row, resolver, oi["other1_col"]))
    other2 = parse_other_income(_get(row, resolver, oi["other2_col"]))
    va = _get(row, resolver, oi["va_benefits_col"], "")

    if joint:
        # Joint: VA Benefits first, then debtor other income, then spouse other income
        desc_idx = 1
        if va:
            lines.append(f"OtherDesc{desc_idx}=VA Benefits")
            lines.append(f"OtherAmtD{desc_idx}={format_amount(va)}")
            desc_idx += 1
        if other1:
            lines.append(f"OtherDesc{desc_idx}={other1['description']}")
            desc_idx += 1
        # Spouse other income
        spouse_other = parse_other_income(_get(row, resolver, "Your spouse's average monthly income from other sources 1"))
        if spouse_other:
            lines.append(f"OtherDesc{desc_idx}={spouse_other['description']}")
            lines.append(f"OtherAmtS{desc_idx}={format_amount(spouse_other['amount'])}")
            desc_idx += 1
    else:
        if other1:
            lines.append(f"OtherDesc1={other1['description']}")
            lines.append(f"OtherAmtD1={format_amount(other1['amount'])}")
        if other2:
            lines.append(f"OtherDesc2={other2['description']}")
        if va:
            lines.append(f"OtherDesc3=VA Benefits")
            lines.append(f"OtherAmtD3={format_amount(va)}")

    lines.append("")
    lines.append("")

    return lines


# ---------------------------------------------------------------------------
# [Expense] section
# ---------------------------------------------------------------------------

def build_expense_section(row, mapping, resolver):
    cfg = mapping["expenses"]
    joint = _is_joint(row, resolver)
    ordered_lines = []

    # Rent: for homeowners, use Mortgage value; for renters, use Rent
    rent_val = _get(row, resolver, "Rent")
    mortgage_val = _get(row, resolver, "Mortgage")
    housing = _get(row, resolver, "Real Estate")
    if housing == "Own" and mortgage_val:
        ordered_lines.append(f"Rent={format_amount(mortgage_val)}")
    else:
        ordered_lines.append(f"Rent={format_amount(rent_val)}")

    ordered_lines.append(f"TaxesIncluded=0")
    ordered_lines.append(f"InsuranceInc=0")
    ordered_lines.append(f"Electricity={format_amount(_get(row, resolver, cfg['fields']['Electricity']['csv_col']))}")
    ordered_lines.append(f"Water={format_amount(_get(row, resolver, cfg['fields']['Water']['csv_col']))}")

    # Telephone: use "Cable, Internet, and Cell Phone" combined field if available
    cable_combined = _get(row, resolver, "Cable, Internet, and Cell Phone")
    if cable_combined:
        ordered_lines.append(f"Telephone={format_amount(cable_combined)}")
    else:
        ordered_lines.append(f"Telephone={format_amount(_get(row, resolver, cfg['fields']['Telephone']['csv_col']))}")
        ordered_lines.append(f"UtilityDsc1=Cable and/or Internet")
        ordered_lines.append(f"UtilityAmt1={format_amount(_get(row, resolver, cfg['fields']['UtilityAmt1']['csv_col']))}")

    ordered_lines.append(f"Food={format_amount(_get(row, resolver, cfg['fields']['Food']['csv_col']))}")
    ordered_lines.append(f"Clothing={format_amount(_get(row, resolver, cfg['fields']['Clothing']['csv_col']))}")
    ordered_lines.append(f"Medical={format_amount(_get(row, resolver, cfg['fields']['Medical']['csv_col']))}")
    ordered_lines.append(f"Transportation={format_amount(_get(row, resolver, cfg['fields']['Transportation']['csv_col']))}")
    ordered_lines.append(f"Recreation={format_amount(_get(row, resolver, cfg['fields']['Recreation']['csv_col']))}")
    ordered_lines.append(f"Charity={format_amount(_get(row, resolver, cfg['fields']['Charity']['csv_col']))}")
    ordered_lines.append(f"InsuranceHealth={format_amount(_get(row, resolver, cfg['fields']['InsuranceHealth']['csv_col']))}")
    ordered_lines.append(f"InsuranceAuto={format_amount(_get(row, resolver, cfg['fields']['InsuranceAuto']['csv_col']))}")
    ordered_lines.append(f"InstallmentAuto={format_amount(_get(row, resolver, cfg['fields']['InstallmentAuto']['csv_col']))}")

    # Other installment payments
    inst_cfg = cfg["installment_payments"]
    inst_idx = 1
    for col_ref in inst_cfg["csv_cols"]:
        parsed = parse_other_expense(_get(row, resolver, col_ref))
        if parsed:
            ordered_lines.append(f"InstallmentDsc{inst_idx}={parsed['description']}")
            ordered_lines.append(f"InstallmentAmt{inst_idx}={format_amount(parsed['amount'])}")
            inst_idx += 1

    # Alimony (individual filing only - joint filing handles through SFA)
    if not joint:
        alimony_val = _get(row, resolver, cfg['fields']['Alimony']['csv_col'])
        if alimony_val:
            ordered_lines.append(f"Alimony={format_amount(alimony_val)}")

    # Other expenses
    oe_cfg = cfg["other_expenses"]
    oe_idx = 1
    for col_ref in oe_cfg["csv_cols"]:
        parsed = parse_other_expense(_get(row, resolver, col_ref))
        if parsed:
            ordered_lines.append(f"OtherDsc{oe_idx}={parsed['description']}")
            ordered_lines.append(f"OtherAmt{oe_idx}={format_amount(parsed['amount'])}")
            oe_idx += 1

    # HOA dues and Home Equity Loan (for homeowners)
    if housing == "Own":
        hoa = _get(row, resolver, "Condo/HOA Dues")
        heloc = _get(row, resolver, "2nd Mortgage / Home Equity Loan / HELOC")
        if hoa:
            ordered_lines.append(f"HOAdues={format_amount(hoa)}")
        if heloc:
            ordered_lines.append(f"HomeEquityLoan={format_amount(heloc)}")

    ordered_lines.append(f"IncludesOthers=0")
    ordered_lines.append(f"Childcare={format_amount(_get(row, resolver, cfg['fields']['Childcare']['csv_col']))}")
    ordered_lines.append(f"PersonalCare={format_amount(_get(row, resolver, cfg['fields']['PersonalCare']['csv_col']))}")
    ordered_lines.append(f"InstallmentAuto2={format_amount(_get(row, resolver, cfg['fields']['InstallmentAuto2']['csv_col']))}")

    # OtherRealHOA (duplicate of HOA for some reason in MyCaseInfo)
    if housing == "Own":
        hoa = _get(row, resolver, "Condo/HOA Dues")
        if hoa:
            ordered_lines.append(f"OtherRealHOA={format_amount(hoa)}")

    ordered_lines.append(f"ExpChangeYES=0")

    ordered_lines.append("")
    ordered_lines.append("")
    return ordered_lines


# ---------------------------------------------------------------------------
# [MTInc] section - Monthly Income (Means Test)
# ---------------------------------------------------------------------------

def build_mtinc_section(row, mapping, resolver):
    cfg = mapping["income"]["other_income"]
    joint = _is_joint(row, resolver)
    header = "Type,DorS,ThruDate,Method,HasExp,Description,Inc1,Inc2,Inc3,Inc4,Inc5,Inc6,Exp1,Exp2,Exp3,Exp4,Exp5,Exp6,IncSame6,ExpSame6,MTIID,Remarks"
    lines = [header]

    now = datetime.now()
    thru_date = f"01/31/{now.year}"
    base_mtiid = 368924

    if joint:
        # Joint filing: W2 employer entries, then self-employment, then other income
        mtiid = 369010

        # Detect debtor and spouse employment type
        debtor_w2 = _get(row, resolver, mapping["income"]["debtor"]["employer_col"], "")
        debtor_biz_name = _get(row, resolver, "What's the name of your business?", "")
        spouse_w2 = _get(row, resolver, "Your Spouse's Current Employer", "")
        spouse_biz = _get(row, resolver, "What's the name of your spouse's business?")

        # Type 1: W2 debtor employer
        if debtor_w2:
            emp_q = f'"{debtor_w2}"' if ',' in debtor_w2 else debtor_w2
            mtiid += 1
            lines.append(f"1,1,{thru_date},1,0,{emp_q},0.00,0.00,0.00,0.00,0.00,0.00,0,0,0,0,0,0,0,0,{mtiid},")

        # Type 1: W2 spouse employer
        if spouse_w2:
            emp_q = f'"{spouse_w2}"' if ',' in spouse_w2 else spouse_w2
            mtiid += 1
            lines.append(f"1,2,{thru_date},1,0,{emp_q},0.00,0.00,0.00,0.00,0.00,0.00,0,0,0,0,0,0,0,0,{mtiid},")
        elif spouse_biz:
            # Type 1 entry for self-employed spouse (business name as employer)
            biz_q = f'"{spouse_biz}"' if ',' in spouse_biz else spouse_biz
            mtiid += 1
            lines.append(f"1,2,{thru_date},1,0,{biz_q},0.00,0.00,0.00,0.00,0.00,0.00,0,0,0,0,0,0,0,0,{mtiid},")

        # Type 2: Debtor self-employment income
        mtiid = 369036
        debtor_biz_income = _get(row, resolver, "Average monthly income from operation of this business", "")
        if debtor_biz_name and debtor_biz_income:
            biz_q = f'"{debtor_biz_name}"' if ',' in debtor_biz_name else debtor_biz_name
            mtiid += 1
            lines.append(f"2,1,{thru_date},2,1,{biz_q},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(debtor_biz_income)},1.00,{mtiid},")

        # Type 2: Spouse self-employment income
        spouse_biz_income = _get(row, resolver, "Average monthly income from operation of your spouse's business")
        if spouse_biz and spouse_biz_income:
            biz_q = f'"{spouse_biz}"' if ',' in spouse_biz else spouse_biz
            mtiid += 1
            lines.append(f"2,2,{thru_date},2,1,{biz_q},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(spouse_biz_income)},1.00,{mtiid},")

        spouse_biz2 = _get(row, resolver, "What's the name of this additional business?#2")
        if not spouse_biz2:
            spouse_biz2 = _get(row, resolver, "What's the name of this additional business?")
        spouse_biz2_income = _get(row, resolver, "Average monthly income from operation of this additional business#2")
        if not spouse_biz2_income:
            spouse_biz2_income = _get(row, resolver, "Average monthly income from operation of this additional business")
        if spouse_biz2 and spouse_biz2_income:
            biz2_q = f'"{spouse_biz2}"' if ',' in spouse_biz2 else spouse_biz2
            mtiid += 1
            lines.append(f"2,2,{thru_date},2,1,{biz2_q},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(spouse_biz2_income)},1.00,{mtiid},")

        # Type 8: Other income - debtor sources first
        mtiid = 369039
        va = _get(row, resolver, cfg["va_benefits_col"])
        other1 = parse_other_income(_get(row, resolver, cfg["other1_col"]))

        if va:
            mtiid += 1
            lines.append(f"8,1,{thru_date},2,0,VA Benefits,0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(va)},0.00,{mtiid},")
        if other1:
            mtiid += 1
            lines.append(f"8,1,{thru_date},2,0,{other1['description']},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(other1['amount'])},0.00,{mtiid},")

        # Spouse other income (DorS=2)
        spouse_other = parse_other_income(_get(row, resolver, "Your spouse's average monthly income from other sources 1"))
        if spouse_other:
            mtiid += 1
            lines.append(f"8,2,{thru_date},2,0,{spouse_other['description']},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(spouse_other['amount'])},0.00,{mtiid},")
    else:
        # Individual filing: standard income sources
        sources = [
            ("5", "Retirement or Pension", cfg["pension_col"]),
            ("3", "Rental Property", cfg["rental_col"]),
            ("61", "Alimony or Child Support", cfg["alimony_col"]),
            ("s", "Unemployment Compensation", cfg["unemployment_col"]),
            ("s", "Social Security", cfg["social_security_col"]),
        ]

        mtiid = base_mtiid
        for type_code, desc, col_ref in sources:
            val = _get(row, resolver, col_ref)
            if val:
                mtiid += 1
                if mtiid == 368926:
                    mtiid = 368927
                has_exp = "1" if desc == "Rental Property" else "0"
                lines.append(f"{type_code},1,{thru_date},2,{has_exp},{desc},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(val)},0.00,{mtiid},")

        other1 = parse_other_income(_get(row, resolver, cfg["other1_col"]))
        other2 = parse_other_income(_get(row, resolver, cfg["other2_col"]))
        va = _get(row, resolver, cfg["va_benefits_col"])

        if other1:
            mtiid += 1
            lines.append(f"8,1,{thru_date},2,0,{other1['description']},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(other1['amount'])},0.00,{mtiid},")
        if other2:
            mtiid += 1
            lines.append(f"8,1,{thru_date},2,0,{other2['description']},0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(other2.get('amount', '0'))},0.00,{mtiid},")
        if va:
            mtiid += 1
            lines.append(f"8,1,{thru_date},2,0,VA Benefits,0,0,0,0,0,0,0,0,0,0,0,0,{format_amount(va)},0.00,{mtiid},")

    return lines
