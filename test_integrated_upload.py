#!/usr/bin/env python3
"""Test script to verify the integrated upload and remove functionality."""

def test_integrated_upload_process():
    """Test the complete upload process with duplicate filtering and remove lines."""

    # Simulate existing content in raw_stripped.txt
    current_content = """This is a test line 1
This is a test line 2
This is a duplicate line
Existing line 4
Line with spam content
Another line with spam"""

    # Simulate new content to upload (after stripping)
    stripped_content = """This is a test line 1
This is a test line 2
This is a duplicate line
Another unique line
Brand new line
Line with spam content
Fresh content"""

    # Simulate remove strings configured
    remove_strings = ["spam", "unwanted"]

    print("=== INTEGRATED UPLOAD TEST ===")
    print()

    # Step 1: Duplicate filtering
    print("Step 1: Duplicate Filtering")
    existing_lines = set()
    if current_content:
        existing_lines = set(line.strip() for line in current_content.split('\n') if line.strip())

    print("Existing lines:")
    for line in sorted(existing_lines):
        print(f"  '{line}'")

    new_lines_list = [line.strip() for line in stripped_content.split('\n') if line.strip()]
    unique_new_lines = [line for line in new_lines_list if line not in existing_lines]

    print("\nNew lines to upload:")
    for line in new_lines_list:
        print(f"  '{line}'")

    print("\nUnique new lines (after duplicate filtering):")
    for line in unique_new_lines:
        print(f"  '{line}'")

    duplicates_filtered = len(new_lines_list) - len(unique_new_lines)
    print(f"\nDuplicate filtering: {duplicates_filtered} duplicates removed, {len(unique_new_lines)} unique lines added")

    # Step 2: Simulate adding new content
    if unique_new_lines:
        new_unique_content = '\n'.join(unique_new_lines)
        if current_content:
            content_after_upload = current_content + "\n" + new_unique_content
        else:
            content_after_upload = new_unique_content
    else:
        content_after_upload = current_content

    print("\nContent after upload:")
    for line in content_after_upload.split('\n'):
        if line.strip():
            print(f"  '{line}'")

    # Step 3: Apply remove lines logic
    print("\nStep 3: Remove Lines Logic")
    print(f"Remove strings: {remove_strings}")

    lines_after_upload = content_after_upload.split('\n')
    original_count_after_upload = len(lines_after_upload)
    kept_lines = []

    for line in lines_after_upload:
        should_remove = False
        line_lower = line.lower()

        for remove_string in remove_strings:
            if remove_string.lower() in line_lower:
                should_remove = True
                break

        if not should_remove:
            kept_lines.append(line)

    removed_by_filters = original_count_after_upload - len(kept_lines)

    print(f"\nRemove lines processing: {removed_by_filters} lines removed, {len(kept_lines)} lines remaining")

    print("\nFinal content after remove lines:")
    for line in kept_lines:
        if line.strip():
            print(f"  '{line}'")

    # Summary
    print("\n=== SUMMARY ===")
    print(f"Original upload lines: {len(new_lines_list)}")
    print(f"Duplicates filtered: {duplicates_filtered}")
    print(f"Lines added to dataset: {len(unique_new_lines)}")
    print(f"Lines removed by filters: {removed_by_filters}")
    print(f"Final dataset size: {len(kept_lines)}")

    # Expected results verification
    expected_final_lines = [
        "This is a test line 1",
        "This is a test line 2",
        "This is a duplicate line",
        "Existing line 4",
        "Another unique line",
        "Brand new line",
        "Fresh content"
    ]

    actual_final_lines = [line for line in kept_lines if line.strip()]

    if sorted(actual_final_lines) == sorted(expected_final_lines):
        print("\n✅ Test PASSED: Integrated functionality works correctly!")
        return True
    else:
        print(f"\n❌ Test FAILED: Expected {expected_final_lines}, got {actual_final_lines}")
        return False

if __name__ == "__main__":
    test_integrated_upload_process()
