#!/usr/bin/env python3
"""Test script to verify duplicate checking functionality."""

def test_duplicate_checking():
    """Test the duplicate checking logic."""

    # Simulate existing content
    current_content = """This is a test line 1
This is a test line 2
This is a duplicate line
Existing line 4"""

    # Simulate new content to upload
    stripped_content = """This is a test line 1
This is a test line 2
This is a duplicate line
Another unique line
Brand new line"""

    # Split content into lines for duplicate checking
    existing_lines = set()
    if current_content:
        existing_lines = set(line.strip() for line in current_content.split('\n') if line.strip())

    print("Existing lines:")
    for line in sorted(existing_lines):
        print(f"  '{line}'")

    # Split new content into lines and filter out duplicates
    new_lines_list = [line.strip() for line in stripped_content.split('\n') if line.strip()]
    unique_new_lines = [line for line in new_lines_list if line not in existing_lines]

    print("\nNew lines to upload:")
    for line in new_lines_list:
        print(f"  '{line}'")

    print("\nUnique new lines (after filtering duplicates):")
    for line in unique_new_lines:
        print(f"  '{line}'")

    print(f"\nOriginal new lines count: {len(new_lines_list)}")
    print(f"Unique new lines count: {len(unique_new_lines)}")
    print(f"Duplicates filtered: {len(new_lines_list) - len(unique_new_lines)}")

    # Verify expected results
    expected_unique = ["Another unique line", "Brand new line"]
    if unique_new_lines == expected_unique:
        print("\n✅ Test PASSED: Duplicate checking works correctly!")
    else:
        print(f"\n❌ Test FAILED: Expected {expected_unique}, got {unique_new_lines}")

if __name__ == "__main__":
    test_duplicate_checking()
