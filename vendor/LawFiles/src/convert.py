#!/usr/bin/env python3
"""
convert.py - Main entry point for CSV -> BCI conversion.

Usage:
    python src/convert.py <input.csv> [output.bci] [--row N]

Architecture:
    1. Reads field_mapping.json and defaults.json from config/
    2. Reads the CSV file (place CSVs in data/sample/)
    3. Calls each section builder independently
    4. Writes the assembled .bci file (default: output/)
    5. Generates a conversion report (what mapped, what didn't)

Each section is built independently so problems can be isolated and fixed
one at a time without affecting other sections.
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from csv_reader import read_csv, HeaderResolver
from bci_writer import BCIWriter
from section_builders import (
    build_file_section,
    build_case_section,
    build_debtor_section,
    build_joint_section,
    build_other_names_section,
    build_dependant_section,
    build_prior_cases_section,
    build_schab_section,
    build_schdef_section,
    build_empty_section,
    build_sfa_section,
    build_income_section,
    build_expense_section,
    build_mtinc_section,
    _reset_link_counter,
    _reset_clink_counter,
)


def load_config():
    """Load all configuration files."""
    config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")
    with open(os.path.join(config_dir, "field_mapping.json"), encoding="utf-8") as f:
        mapping = json.load(f)
    with open(os.path.join(config_dir, "defaults.json"), encoding="utf-8") as f:
        defaults = json.load(f)
    return mapping, defaults


def convert_row(row, mapping, defaults, resolver, show_progress=False):
    """
    Convert a single CSV row into a BCIWriter object.

    Each section is wrapped in a try/except so that a failure in one
    section doesn't prevent the rest from being generated. This makes
    it easy to debug section by section.

    resolver: HeaderResolver that maps header names to column indices.
    """
    writer = BCIWriter()
    report = {"success": [], "errors": [], "warnings": []}

    # Reset ID counters for each conversion
    _reset_link_counter(3796460)
    _reset_clink_counter(3291287)

    # Define all sections in order, matching reference .bci file
    section_builders = [
        ("File",          build_file_section),
        ("Case",          build_case_section),
        ("Debtor",        build_debtor_section),
        ("Joint",         build_joint_section),
        ("Other Names",   build_other_names_section),
        ("Dependant",     build_dependant_section),
        ("Prior Cases",   build_prior_cases_section),
        ("Related Cases", lambda r, m, res: []),
        ("SchAB",         build_schab_section),
        ("SchDEF",        build_schdef_section),
        ("ANP",           lambda r, m, res: ["Name,Address1,Address2,Address3,City,State,Zip,Phone,AccountNo,CLinkID"]),
        ("SchH",          lambda r, m, res: ["Name,Address1,Address2,Address3,City,State,Zip,CLinkID,LLinkID"]),
        ("SchG",          lambda r, m, res: ["LLinkID,Name,Address1,Address2,Address3,City,State,Zip,Intention,Description,Eviction"]),
        ("PriorSpouses",  lambda r, m, res: ["SpouseID,Name,Address1,Address2,City,State,ZIP,MarriageDates,StateResided,SpouseDS,Comment"]),
        ("SFA",           build_sfa_section),
        ("Income",        build_income_section),
        ("Employers",     lambda r, m, res: []),
        ("Expense",       build_expense_section),
        ("MTInc",         build_mtinc_section),
    ]

    for section_name, builder_fn in section_builders:
        if show_progress:
            print(f"   • Building [{section_name}]...", flush=True)
        try:
            lines = builder_fn(row, mapping, resolver)
            writer.add_section(section_name, lines)
            report["success"].append(section_name)
            if show_progress:
                print(f"     ✓ [{section_name}]", flush=True)
        except Exception as e:
            # On error, add empty section so file structure is preserved
            writer.add_section(section_name, [f"; ERROR: {str(e)}"])
            report["errors"].append({
                "section": section_name,
                "error": str(e),
                "type": type(e).__name__,
            })
            print(f"  ⚠ ERROR in [{section_name}]: {type(e).__name__}: {e}", file=sys.stderr, flush=True)

    return writer, report


def generate_report(report, output_path):
    """Generate a human-readable conversion report."""
    report_path = output_path.replace(".bci", "_report.txt")
    with open(report_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("BCI CONVERSION REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"✅ Successful sections ({len(report['success'])}):\n")
        for s in report["success"]:
            f.write(f"   • {s}\n")

        if report["errors"]:
            f.write(f"\n❌ Failed sections ({len(report['errors'])}):\n")
            for err in report["errors"]:
                f.write(f"   • [{err['section']}] {err['type']}: {err['error']}\n")

        if report["warnings"]:
            f.write(f"\n⚡ Warnings ({len(report['warnings'])}):\n")
            for w in report["warnings"]:
                f.write(f"   • {w}\n")

        f.write("\n" + "=" * 60 + "\n")
        total = len(report["success"]) + len(report["errors"])
        pct = len(report["success"]) / total * 100 if total else 0
        f.write(f"RESULT: {len(report['success'])}/{total} sections OK ({pct:.0f}%)\n")

    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="Convert Gravity Forms CSV export to MyCaseInfo .bci file"
    )
    parser.add_argument("input_csv", help="Path to Gravity Forms CSV export")
    parser.add_argument("output_bci", nargs="?", default=None,
                        help="Output .bci file path (default: derived from input)")
    parser.add_argument("--row", type=int, default=0,
                        help="Which CSV data row to convert (0-based, default: 0 = first)")
    parser.add_argument("--no-report", action="store_true",
                        help="Skip generating the conversion report")

    args = parser.parse_args()

    # Derive output path
    if args.output_bci is None:
        base = os.path.splitext(args.input_csv)[0]
        args.output_bci = base + ".bci"

    print(f"📄 Loading config...", flush=True)
    mapping, defaults = load_config()

    print(f"📂 Reading CSV: {args.input_csv}", flush=True)
    headers, rows = read_csv(args.input_csv)
    print(f"   Found {len(rows)} data row(s), {len(headers)} columns", flush=True)

    if args.row >= len(rows):
        print(f"❌ Row {args.row} does not exist (only {len(rows)} rows)", flush=True)
        sys.exit(1)

    row = rows[args.row]
    resolver = HeaderResolver(headers)
    first = row.get(resolver.resolve("Your Name (First)"), "")
    middle = row.get(resolver.resolve("Your Name (Middle)"), "")
    last = row.get(resolver.resolve("Your Name (Last)"), "")
    suffix = row.get(resolver.resolve("Your Name (Suffix)"), "")
    debtor_name = f"{first} {middle} {last} {suffix}".strip()
    print(f"   Converting row {args.row}: {debtor_name}", flush=True)

    print(f"\n🔧 Building BCI sections...", flush=True)
    writer, report = convert_row(row, mapping, defaults, resolver, show_progress=True)

    print(f"\n💾 Writing: {args.output_bci}", flush=True)
    writer.write(args.output_bci)

    if not args.no_report:
        report_path = generate_report(report, args.output_bci)
        print(f"📋 Report:  {report_path}", flush=True)

    # Summary
    ok = len(report["success"])
    fail = len(report["errors"])
    total = ok + fail
    print(f"\n{'✅' if fail == 0 else '⚠'} Done: {ok}/{total} sections converted successfully", flush=True)

    if fail > 0:
        print(f"   Fix errors in the failing sections and re-run.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
