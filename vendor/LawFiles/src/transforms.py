"""
transforms.py - Data transformation functions for CSV -> BCI conversion.

Each function takes a raw CSV value and returns a BCI-formatted value.
Add new transforms here and reference them by name in field_mapping.json.
"""

import json
import os

# Load config once - config/ is one level up from src/
_config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")

def _load_json(filename):
    with open(os.path.join(_config_dir, filename), encoding="utf-8") as f:
        return json.load(f)

_mapping = _load_json("field_mapping.json")
STATE_MAP = _mapping.get("state_abbreviations", {})
SUFFIX_MAP = _mapping.get("suffix_map", {})


def state_to_abbrev(value):
    """Convert full state name to 2-letter abbreviation. Pass through if already abbreviated."""
    if not value:
        return ""
    value = value.strip()
    if len(value) == 2:
        return value.upper()
    return STATE_MAP.get(value, value)


def suffix_to_generation(value):
    """Convert suffix like 'Junior' to BCI generation code like 'JR'."""
    if not value:
        return ""
    return SUFFIX_MAP.get(value.strip(), value.strip().upper())


def format_zip_debtor(value):
    """
    Format ZIP for the Debtor section: XXXXX-00000 (always zeroes out +4).
    The BCI reference shows the debtor ZIP as '33441-00000' even though
    the CSV has '33441 3617'.
    """
    if not value:
        return ""
    value = value.strip().replace("-", " ")
    parts = value.split()
    zip5 = parts[0].strip() if parts else value
    return f"{zip5}-00000"


def format_zip_10(value):
    """
    Format ZIP to 10-char BCI format: XXXXX-XXXXX
    Input can be: '33441 3617', '33441-3617', '33441', '334413617'
    Output: '33441-03617' or '33441-00000' (padded)
    """
    if not value:
        return ""
    value = value.strip().replace(" ", "-").replace("  ", "-")
    parts = value.split("-")
    zip5 = parts[0].strip()
    zip4 = parts[1].strip() if len(parts) > 1 else "00000"
    # Pad zip4 to 5 digits if needed (e.g., '3617' -> '03617')  -- actually BCI uses varied formats
    # Looking at the reference: 33441-00000, 33441 7433 -> keep as-is with space replaced
    return f"{zip5}-{zip4.zfill(5)}" if zip4 else f"{zip5}"


def format_zip_bci_space(value):
    """Format ZIP keeping space separator as in some BCI fields: '33441 7433'."""
    if not value:
        return ""
    return value.strip().replace("-", " ")


def parse_pipe_list(value):
    """Split a pipe-delimited string into a list. Returns empty list if blank."""
    if not value or not value.strip():
        return []
    return [item.strip() for item in value.split("|")]


def parse_other_names(value):
    """
    Parse other names from CSV. 
    Input: 'Johnny|Doe-Joe' (pipe-delimited list of names in a single field)
    Output: list of tuples [(type, raw_name, display_name, middle, suffix), ...]
    BCI format: index,type,RawName,DisplayName,Middle,Suffix
    
    Note: The CSV stores ALL other names in one pipe-delimited field.
    The BCI reference shows them as a single combined entry:
      1,aka,Doe-Joe,Johnny,,
    So we combine them into one line with Last-First style.
    """
    if not value:
        return []
    names = parse_pipe_list(value)
    if len(names) == 0:
        return []
    # Names are pipe-delimited: "DisplayName|RawName" (e.g., "Johnny|Joe-Doe")
    # BCI format: type,RawName,DisplayName,Middle,Suffix
    # col1 = raw/maiden name (2nd in pipe), col2 = display/first name (1st in pipe)
    if len(names) >= 2:
        col1 = names[1]  # raw/maiden name
        col2 = names[0]  # display name
    else:
        col1 = names[0]
        col2 = names[0]
    return [("aka", col1, col2, "", "")]


def parse_dependants(value):
    """
    Parse dependent info from CSV.
    Input: '12|Son' (age|relationship)
    Output: (relationship, age)
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    if len(parts) >= 2:
        return (parts[1].strip(), parts[0].strip())
    return None


_VEHICLE_PLACEHOLDERS = {"n/a", "na", "none", "no", "n/a.", "none.", "0", ""}

def parse_vehicle(value):
    """
    Parse vehicle info from CSV.
    Input: '2020|Honda|Civic|60k'
    Output: dict with year, make, model, mileage
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    # Skip placeholder entries (all fields are N/A, None, No, etc.)
    real_parts = [p.strip() for p in parts if p.strip().lower() not in _VEHICLE_PLACEHOLDERS]
    if not real_parts:
        return None
    result = {
        "year": parts[0].strip() if len(parts) > 0 else "",
        "make": parts[1].strip() if len(parts) > 1 else "",
        "model": parts[2].strip() if len(parts) > 2 else "",
        "mileage": parts[3].strip().replace(",", "") if len(parts) > 3 else "",
    }
    return result


def parse_bank_account(value):
    """
    Parse bank account info from CSV.
    Input: 'Chase Bank|Checking|9015'
    Output: dict with bank_name, account_type, last4
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "bank_name": parts[0].strip() if len(parts) > 0 else "",
        "account_type": parts[1].strip() if len(parts) > 1 else "",
        "last4": parts[2].strip() if len(parts) > 2 else "",
    }


def parse_investment_account(value):
    """
    Parse investment/retirement account info.
    Input examples:
      - 'Charles Schwabb Investment Account|1234'
      - 'IRA|Schwabb|1234'
    Output: dict with account_type (optional), name, account_number
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    if len(parts) >= 3:
        return {
            "account_type": parts[0].strip(),
            "name": parts[1].strip(),
            "account_number": parts[2].strip(),
        }

    return {
        "account_type": "",
        "name": parts[0].strip() if len(parts) > 0 else "",
        "account_number": parts[1].strip() if len(parts) > 1 else "",
    }


def parse_creditor(value):
    """
    Parse non-consumer debt / creditor info.
    Input: 'Amex|506|111 Amex Rd.|Amextown|FL|33331'
    Output: dict with name, amount, address, city, state, zip
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "amount": parts[1].strip().replace(",", "") if len(parts) > 1 else "",
        "address": parts[2].strip() if len(parts) > 2 else "",
        "city": parts[3].strip() if len(parts) > 3 else "",
        "state": parts[4].strip() if len(parts) > 4 else "",
        "zip": parts[5].strip() if len(parts) > 5 else "",
    }


def parse_tax_debt(value):
    """
    Parse tax debt info.
    Input: 'IRS|2017-2019, and 2023|1040.99'
    Output: dict with entity, years, amount
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "entity": parts[0].strip() if len(parts) > 0 else "",
        "years": parts[1].strip() if len(parts) > 1 else "",
        "amount": parts[2].strip() if len(parts) > 2 else "",
    }


def parse_lawsuit(case_type, location, status, has_number, case_number):
    """
    Parse lawsuit/court case info from repeater fields.
    Returns dict or None.
    """
    if not case_type or not case_type.strip():
        return None
    loc_parts = parse_pipe_list(location) if location else ["", ""]
    # Map status to BCI code: N/A=0, Pending=1, On appeal=2, Concluded=3
    status_map = {"N/A": 0, "Pending": 1, "On appeal": 2, "Concluded": 3}
    return {
        "case_type": case_type.strip(),
        "county": loc_parts[0].strip() if len(loc_parts) > 0 else "",
        "state": loc_parts[1].strip() if len(loc_parts) > 1 else "",
        "status": status_map.get(status.strip(), 0) if status else 0,
        "case_number": case_number.strip() if case_number else "",
    }


def parse_insider_loan(value):
    """
    Parse insider loan repayment.
    Input: 'Rita Doe|2/11/2024|6768|Repaying loan to my cousin Rita'
    Output: dict
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "date": parts[1].strip() if len(parts) > 1 else "",
        "amount": parts[2].strip() if len(parts) > 2 else "",
        "description": parts[3].strip() if len(parts) > 3 else "",
    }


def parse_seizure(value):
    """
    Parse repossession/foreclosure/garnishment/levy info.
    Input: 'Ally Bank|2/12/2024|My Toyota Truck was repossessed'
    Output: dict with name, date, description
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "date": parts[1].strip() if len(parts) > 1 else "",
        "description": parts[2].strip() if len(parts) > 2 else "",
    }


def parse_gift(value):
    """
    Parse gift info.
    Input: 'Kelly Doe|2/16/2024|6770|Neice 16th Birthday present'
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "date": parts[1].strip() if len(parts) > 1 else "",
        "amount": parts[2].strip() if len(parts) > 2 else "",
        "description": parts[3].strip() if len(parts) > 3 else "",
    }


def parse_loss(value):
    """
    Parse loss info.
    Input: 'Theft from stolen iphone|2/17/2024|6771'
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "description": parts[0].strip() if len(parts) > 0 else "",
        "date": parts[1].strip() if len(parts) > 1 else "",
        "amount": parts[2].strip().replace(",", "") if len(parts) > 2 else "",
    }


def parse_closed_account(value):
    """
    Parse closed/transferred account info.
    Input: 'Bank of America|Checking|2/19/2024|3.67'
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "type": parts[1].strip() if len(parts) > 1 else "",
        "date": parts[2].strip() if len(parts) > 2 else "",
        "balance": parts[3].strip() if len(parts) > 3 else "",
    }


def parse_other_income(value):
    """
    Parse other income source.
    Input: 'Uber|307'
    Output: dict with description, amount
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "description": parts[0].strip() if len(parts) > 0 else "",
        "amount": parts[1].strip() if len(parts) > 1 else "",
    }


def parse_other_expense(value):
    """
    Parse other expense.
    Input: 'Netflix subscription|328'
    Output: dict with description, amount
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "description": parts[0].strip() if len(parts) > 0 else "",
        "amount": parts[1].strip() if len(parts) > 1 else "",
    }


def parse_support_recipient(value):
    """
    Parse support recipient (child support / alimony).
    Input: 'Baby Mama|111 Babymama Rd.|Babymamatown|CA|91111'
    """
    if not value or not value.strip():
        return None
    parts = parse_pipe_list(value)
    return {
        "name": parts[0].strip() if len(parts) > 0 else "",
        "address": parts[1].strip() if len(parts) > 1 else "",
        "city": parts[2].strip() if len(parts) > 2 else "",
        "state": parts[3].strip() if len(parts) > 3 else "",
        "zip": parts[4].strip() if len(parts) > 4 else "",
    }


def format_amount(value):
    """Ensure a value is formatted as X.XX for BCI amounts."""
    if not value:
        return "0.00"
    value = value.strip().replace(",", "").replace("$", "")
    try:
        return f"{float(value):.2f}"
    except ValueError:
        return value


def format_date_leading_zeros(value):
    """
    Normalize a date like '2/12/2024' to '02/12/2024' (with leading zeros).
    MyCaseInfo stores dates with leading zeros on month and day.
    """
    if not value or "/" not in value:
        return value
    try:
        from datetime import datetime
        dt = datetime.strptime(value.strip(), "%m/%d/%Y")
        return dt.strftime("%m/%d/%Y")
    except (ValueError, TypeError):
        return value


def extract_name_word(name):
    """
    Extract a meaningful word from a name for synthetic address construction.
    MyCaseInfo uses a key word from the entity name for fake addresses.
    - Skips common titles (Mr., Mrs., Ms., Dr.)
    - For multi-word names, uses the first meaningful word
    """
    if not name:
        return "Unknown"
    words = name.strip().split()
    if not words:
        return "Unknown"
    # Skip common title prefixes
    titles = {"Mr.", "Mrs.", "Ms.", "Dr.", "Jr.", "Sr."}
    for word in words:
        if word not in titles:
            # Strip trailing punctuation (commas, periods, etc.)
            return word.rstrip(".,;:'\"")
    return words[-1].rstrip(".,;:'\"")  # Fallback to last word


# Registry of transform functions by name (referenced in field_mapping.json)
TRANSFORMS = {
    "state_to_abbrev": state_to_abbrev,
    "suffix_to_generation": suffix_to_generation,
    "format_zip_10": format_zip_10,
    "format_zip_debtor": format_zip_debtor,
    "format_amount": format_amount,
}
