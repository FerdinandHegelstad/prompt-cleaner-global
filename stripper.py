#!stripper.py
import re
import sys
from datetime import datetime
from typing import List

def is_date(string: str) -> bool:
    """Return True if the string contains any recognizable date/time pattern.

    This checks for presence *anywhere* in the line (not just exact match).
    Patterns covered include:
      - ISO-8601 (YYYY-MM-DD with optional time and timezone)
      - RFC-1123 (e.g., 'Sat, 19 Oct 2024 19:34:10 +0000')
      - JS Date.toString() (e.g., 'Sat Oct 19 2024 19:34:10 GMT+0000 (Coordinated Universal Time)')
      - Numeric dates with slashes/dots (YYYY/MM/DD, DD/MM/YYYY, MM/DD/YYYY, YYYY.MM.DD)
      - Month-name formats (e.g., 'Oct 19, 2024', '19 Oct 2024', including Norwegian month names)
    """
    s = string.strip()
    if not s:
        return False

    patterns = [
        # ISO-8601 date with optional time and tz
        r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:?\d{2})?)?\b",
        # YYYY/MM/DD or DD/MM/YYYY or MM/DD/YYYY
        r"\b(?:\d{4}[/-]\d{2}[/-]\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{4})\b",
        # YYYY.MM.DD
        r"\b\d{4}\.\d{2}\.\d{2}\b",
        # RFC-1123: Sat, 19 Oct 2024 19:34:10 +0000
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+[+-]\d{4}\b",
        # JS Date.toString(): Sat Oct 19 2024 19:34:10 GMT+0000 (Coordinated Universal Time)
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}(?::\d{2})?\s+GMT[+-]\d{4}(?:\s*\([^)]*\))?",
        # Month name formats (English)
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\b",
        r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{4}\b",
        # Month name formats (Norwegian)
        r"\b(?:jan|feb|mar|apr|mai|jun|jul|aug|sep|okt|nov|des)\.?\s+\d{1,2},?\s+\d{4}\b",
        r"\b\d{1,2}\s+(?:jan|feb|mar|apr|mai|jun|jul|aug|sep|okt|nov|des)\.?\s+\d{4}\b",
    ]

    for pat in patterns:
        if re.search(pat, s, flags=re.IGNORECASE):
            return True
    return False

# Helper to detect "Navn:" lines
def is_navn_line(string: str) -> bool:
    """Return True if the line starts with 'Navn:' (case-insensitive, ignores leading spaces)."""
    return re.match(r"^\s*Navn:\s*", string, flags=re.IGNORECASE) is not None

def strip_file(input_file: str) -> str:
    """Strips dates and empty lines from the input file and writes to a new file.

    Args:
        input_file: Path to the input file.

    Returns:
        Path to the output file.
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    stripped_lines = [
        line.strip()
        for line in lines
        if len(line.strip()) >= 6 and not is_navn_line(line) and not is_date(line)
    ]

    output_file = f"{input_file.rsplit('.', 1)[0]}_stripped.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(stripped_lines))

    return output_file

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python stripper.py <input_file>")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = strip_file(input_file)
    print(f"Stripped file created: {output_file}")