import sys

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python remove_lines.py <file.txt> <param1> <param2> ...")
        sys.exit(1)

    file_path = sys.argv[1]
    params = sys.argv[2:]

    if not params:
        print("No params provided; file unchanged.")
        sys.exit(0)

    remove_lines_containing(file_path, params)
    # No output; file is modified in place