"""
gf_adapter.py — Gravity Forms entry JSON -> converter-input shape.

The existing BCI converter (vendored at vendor/LawFiles/) reads CSV exports
produced by GF's "Export Entries" UI. We don't go through the export — we
hit the GF REST API directly and synthesize the equivalent (headers, [CSVRow])
tuple, which the converter's read_csv() would have produced.

Why synthesize instead of fetching the export?
  - The export endpoint is async + auth'd differently and produces files.
  - Live entries hit the REST API immediately on submission.
  - One canonical adapter means one place to fix when GF changes.

How GF stores entry data:
  - Simple fields:        entry["123"]            -> "value"
  - Compound fields:      entry["123.1"]          -> sub-input value
                          entry["123.geolocation_latitude"]  -> ...
  - List (tabular) field: entry["123"]            -> [{col: val, ...}, ...]
  - List (single-col):    entry["123"]            -> "row1|row2"  or list of strings
  - Checkboxes (multi):   entry["123.1"]          -> "Choice 1 label" if checked, else ""

How the GF CSV export names columns:
  - Simple fields:        "{field.label}"
  - Compound inputs:      "{field.label} ({input.label})"
  - List rows:            "{field.label} 1", "{field.label} 2", ...
                          (the converter discovers these by incrementing N
                          until lookup fails — see build_dependant_section)
  - Per-row cell content: pipe-delimited values in the order of field.choices
                          e.g. "Age|Relationship" -> "23 years old|son"
  - Display-only types (section, page, html) are NOT in the CSV.
  - 15 trailing meta columns: Form Source, Created By, Entry Id, etc.

The converter looks up columns by header name via HeaderResolver, so column
ORDER doesn't strictly matter for resolution — but DUPLICATE header order
does (e.g. "Cell Phone" vs "Cell Phone#3"). We preserve schema order to
keep duplicate occurrences stable.
"""

import json

from lib.converter import CSVRow


# Display-only field types that produce no CSV columns.
_SKIP_TYPES = {"section", "page", "html", "captcha"}

# Trailing meta columns appended after all form fields. Each tuple is
#   (CSV header text, entry meta key)
# Order matches what the GF export produces (verified against the real CSV).
_META_COLUMNS = [
    ("Form Source",            "source_id"),
    ("Created By (User Id)",   "created_by"),
    ("Entry Id",               "id"),
    ("Entry Date",             "date_created"),
    ("Date Updated",           "date_updated"),
    ("Source Url",             "source_url"),
    ("Transaction Id",         "transaction_id"),
    ("Payment Amount",         "payment_amount"),
    ("Payment Date",           "payment_date"),
    ("Payment Status",         "payment_status"),
    ("Post Id",                "post_id"),
    ("User Agent",             "user_agent"),
    ("User IP",                "ip"),
    ("Submission Speed (ms)",  "submission_speeds"),
    ("reCAPTCHA Score",        "gravityformsrecaptcha_score"),
]


def _s(v) -> str:
    """Coerce any GF value to the str representation a CSV cell would hold."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return str(v)


def _list_field_columns(field: dict, raw_value):
    """Expand a list field's value into one column per row.

    Header  : "{label} {N}"  (1-based)
    Cell    : pipe-delimited values, in field.choices order for tabular lists
              e.g. choices=[Age, Relationship] + row={Age:"23", Relationship:"son"}
              -> "23|son"
    Empty   : emits one column "{label} 1" with empty value, so the converter's
              column-discovery loop terminates after exactly one miss.
    """
    label = field.get("label", "")
    choices = field.get("choices") or []
    col_order = [c.get("text", "") for c in choices]

    # API may decode JSON for us, or hand back a string.
    value = raw_value
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return [(f"{label} 1", value)]

    if not value:
        return [(f"{label} 1", "")]

    if not isinstance(value, list):
        return [(f"{label} 1", _s(value))]

    out = []
    for i, row in enumerate(value, start=1):
        header = f"{label} {i}"
        if isinstance(row, dict):
            if col_order:
                cell = "|".join(_s(row.get(c, "")) for c in col_order)
            else:
                cell = "|".join(_s(v) for v in row.values())
        else:
            cell = _s(row)
        out.append((header, cell))
    return out


def _compound_field_columns(field: dict, entry: dict):
    """Compound fields (name/address/email/multi-option checkbox).

    Header format depends on field TYPE — verified against the GF CSV export:
      - name, address: "{parent.label} ({input.label})"
                       e.g. "Your Name (First)"
      - email:         only the first input is exported; header is
                       "{parent.label} ({input.label})" e.g. "Your Email
                       Address (Enter Email)". The Confirm Email sub-input
                       is omitted from the CSV.
      - checkbox:      one column per option, header is just "{input.label}"
                       (NO parent prefix) e.g. "Washer".
    """
    label = field.get("label", "")
    ftype = field.get("type")
    inputs = field.get("inputs") or []
    out = []
    for i, inp in enumerate(inputs):
        sub_label = inp.get("label", "")
        sub_id = _s(inp.get("id"))
        value = entry.get(sub_id, "")
        if ftype == "checkbox":
            header = sub_label
        elif ftype == "email":
            if i > 0:
                continue  # skip Confirm Email and any further sub-inputs
            header = f"{label} ({sub_label})"
        else:
            header = f"{label} ({sub_label})"
        out.append((header, _s(value)))
    return out


def _simple_field_columns(field: dict, entry: dict):
    label = field.get("label", "")
    value = entry.get(_s(field.get("id")), "")
    return [(label, _s(value))]


def adapt(form_schema: dict, entry: dict):
    """Build (headers, [CSVRow]) from one GF entry + the form schema.

    Output shape matches csv_reader.read_csv(): the converter's
    convert_row() consumes this directly with no modifications.
    """
    headers: list[str] = []
    values: list[str] = []

    for field in form_schema.get("fields", []):
        ftype = field.get("type")
        if ftype in _SKIP_TYPES:
            continue

        if ftype == "list":
            cols = _list_field_columns(field, entry.get(_s(field.get("id")), ""))
        elif field.get("inputs"):
            cols = _compound_field_columns(field, entry)
        else:
            cols = _simple_field_columns(field, entry)

        for header, cell in cols:
            headers.append(header)
            values.append(cell)

    for header, entry_key in _META_COLUMNS:
        headers.append(header)
        values.append(_s(entry.get(entry_key, "")))

    row = CSVRow(headers, values)
    return headers, [row]
