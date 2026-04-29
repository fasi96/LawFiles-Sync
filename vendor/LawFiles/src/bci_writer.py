"""
bci_writer.py - Writes the .bci output file.

The BCI format is an INI-like text file with:
  - [Section] headers
  - Key=Value pairs  
  - CSV-style data rows (in some sections like SchAB, SchDEF, SFA)
  - Windows line endings (\r\n)
"""


class BCIWriter:
    """Builds a .bci file section by section."""

    def __init__(self):
        self.sections = []  # list of (section_name, content_lines)

    def add_section(self, name, lines):
        """
        Add a complete section.
        name: section header (without brackets)
        lines: list of strings (key=value or raw data lines)
        """
        self.sections.append((name, lines))

    def add_keyvalue_section(self, name, kv_pairs):
        """
        Add a section with key=value pairs.
        kv_pairs: list of (key, value) tuples or dict
        """
        if isinstance(kv_pairs, dict):
            kv_pairs = list(kv_pairs.items())
        lines = [f"{k}={v}" for k, v in kv_pairs]
        self.add_section(name, lines)

    def write(self, filepath):
        """Write the complete .bci file with Windows line endings."""
        with open(filepath, "w", encoding="utf-8", newline="") as f:
            for section_name, lines in self.sections:
                f.write(f"[{section_name}]\r\n")
                for line in lines:
                    f.write(f"{line}\r\n")
                f.write("\r\n")

    def to_string(self):
        """Return the complete .bci content as a string (for debugging)."""
        parts = []
        for section_name, lines in self.sections:
            parts.append(f"[{section_name}]")
            parts.extend(lines)
            parts.append("")
        return "\r\n".join(parts)
