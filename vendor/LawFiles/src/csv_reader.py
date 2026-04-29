"""
csv_reader.py - Read and normalize the Gravity Forms CSV export.

Returns a simple dict-like row object that can be accessed by column index.
Also provides a HeaderResolver to map header names to column indices,
handling duplicate headers via occurrence numbers.
"""

import csv
import os


class CSVRow:
    """Wrapper around a CSV row that allows safe access by column index."""

    def __init__(self, headers, values):
        self.headers = headers
        self.values = values

    def get(self, col_index, default=""):
        """Get value at column index, returning default if empty or out of range."""
        if col_index is None:
            return default
        try:
            val = self.values[col_index]
            return val.strip() if val and val.strip() else default
        except (IndexError, TypeError):
            return default

    def get_header(self, col_index):
        """Get header name at column index."""
        try:
            return self.headers[col_index]
        except (IndexError, TypeError):
            return f"COL_{col_index}"

    def __len__(self):
        return len(self.values)


class HeaderResolver:
    """
    Maps header names to column indices, handling duplicate headers.

    Many Gravity Forms exports have duplicate header names (e.g. debtor vs spouse
    checkboxes share the same header text). We handle this with an occurrence
    number: "Header Name" gets occurrence 0, and "Header Name#2" gets occurrence 1.

    Format in field_mapping.json:
        "Your Name (First)"       -> first occurrence of this header
        "Your Name (First)#2"     -> second occurrence
    """

    # Smart/curly quotes that Gravity Forms sometimes uses in headers
    _QUOTE_MAP = str.maketrans({
        "\u2018": "'",  # LEFT SINGLE QUOTATION MARK
        "\u2019": "'",  # RIGHT SINGLE QUOTATION MARK
        "\u201C": '"',  # LEFT DOUBLE QUOTATION MARK
        "\u201D": '"',  # RIGHT DOUBLE QUOTATION MARK
    })

    @staticmethod
    def _normalize(text):
        """Normalize smart quotes to ASCII equivalents."""
        return text.translate(HeaderResolver._QUOTE_MAP)

    def __init__(self, headers):
        # Build a map: header_name -> [col_idx_0, col_idx_1, ...]
        # Normalize smart quotes so config files can use plain ASCII
        self._header_positions = {}
        for idx, name in enumerate(headers):
            norm = self._normalize(name)
            if norm not in self._header_positions:
                self._header_positions[norm] = []
            self._header_positions[norm].append(idx)

    def resolve(self, header_ref):
        """
        Resolve a header reference to a column index.

        header_ref can be:
            "Header Name"     -> first (0th) occurrence
            "Header Name#2"   -> second occurrence (1-based in the #N suffix)
            123               -> pass-through integer index (backward compat)

        Returns column index (int) or None if not found.
        """
        if isinstance(header_ref, int):
            return header_ref

        if not isinstance(header_ref, str):
            return None

        # Parse occurrence suffix: "Header Name#2" -> ("Header Name", 1)
        occurrence = 0
        name = self._normalize(header_ref)
        if "#" in name:
            parts = name.rsplit("#", 1)
            try:
                occurrence = int(parts[1]) - 1  # #2 means index 1
                name = parts[0]
            except (ValueError, IndexError):
                pass

        positions = self._header_positions.get(name, [])
        if occurrence < len(positions):
            return positions[occurrence]
        return None


def read_csv(filepath):
    """
    Read a Gravity Forms CSV export file.
    Returns: (headers, list_of_CSVRow)

    Note: Gravity Forms exports may have BOM and use UTF-8 encoding.
    """
    rows = []
    with open(filepath, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for raw_row in reader:
            rows.append(CSVRow(headers, raw_row))
    return headers, rows
