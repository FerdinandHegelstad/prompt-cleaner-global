#!text_utils.py
"""
Text processing utilities for cleaning and filtering text files.
Combines functionality from stripper.py and remove_lines.py.
"""

import re
import sys
from datetime import datetime
from typing import List


# Date and text filtering functions (from stripper.py)
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


# Text normalization functions (from normalizer.py)
def normalize(text: str) -> str:
    """Normalizes the text by keeping only word characters and whitespace.

    Args:
        text: The text to normalize.

    Returns:
        Normalized text.
    """
    return re.sub(r'[^\w\s]', '', text)


def build_dedup_key(normalized_text: str) -> str:
    """Builds a deduplication key from an already-normalized string.

    The key removes specific whole words (case-insensitive) that should not
    affect duplicate detection, then collapses whitespace and lowercases for
    stable comparisons.

    Args:
        normalized_text: Text that has already been normalized by `normalize`.

    Returns:
        A deduplication key suitable for equality checks.
    """
    text = (normalized_text or '').strip()
    if not text:
        return ''

    # Remove the words 'player' and 'drinks' as whole words, case-insensitive.
    text = re.sub(r"\b(?:player|drinks)\b", " ", text, flags=re.IGNORECASE)

    # Collapse runs of whitespace and trim.
    text = re.sub(r"\s+", " ", text).strip()

    # Lowercase to ensure case-insensitive comparisons are stable.
    return text.lower()


# Line removal functions (from remove_lines.py)
def remove_lines_containing(file_path: str, params: list[str]) -> None:
    """
    Removes lines from the file that contain any of the provided params as substrings.
    Modifies the file in place.
    """
    try:
        # Read all lines
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Filter lines: keep if NONE of the params are in the line (case-insensitive whole word check)
        kept_lines = []
        for line in lines:
            line_content_lower = line.strip().lower()  # Lowercase for case-insensitive check
            should_remove = False
            for param in params:
                param_lower = param.lower()
                # Check if param is a whole word (surrounded by spaces or at start/end)
                if (f" {param_lower} " in f" {line_content_lower} " or
                    line_content_lower.startswith(f"{param_lower} ") or
                    line_content_lower.endswith(f" {param_lower}") or
                    line_content_lower == param_lower):
                    should_remove = True
                    break
            if not should_remove:
                kept_lines.append(line)  # Keep original line including newline

        # Write back the kept lines
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(kept_lines)

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        sys.exit(1)


# CLI interface for backward compatibility
if __name__ == "__main__":
    if len(sys.argv) == 2:
        # stripper.py interface
        input_file = sys.argv[1]
        output_file = strip_file(input_file)
        print(f"Stripped file created: {output_file}")
    elif len(sys.argv) >= 3:
        # remove_lines.py interface
        file_path = sys.argv[1]
        params = sys.argv[2:]

        if not params:
            print("No params provided; file unchanged.")
            sys.exit(0)

        remove_lines_containing(file_path, params)
        # No output; file is modified in place
    else:
        print("Usage:")
        print("  python text_utils.py <input_file>                    # Strip dates (like stripper.py)")
        print("  python text_utils.py <file.txt> <param1> <param2> ... # Remove lines (like remove_lines.py)")
        sys.exit(1)
