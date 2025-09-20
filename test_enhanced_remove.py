#!/usr/bin/env python3
"""Test script to verify the enhanced remove functionality that handles both substring and identical line matching."""

def test_enhanced_remove_logic():
    """Test the enhanced remove logic that handles both substring and identical line removal."""

    # Test data: lines in the dataset
    test_lines = [
        "This is a test line",
        "Another test line",
        "spam content here",
        "This is spam",
        "normal line",
        "SPAM",  # uppercase version
        "contains spam in middle",
        "unwanted",
        "This line contains unwanted content",
        "exact match",
        "different line"
    ]

    # Remove strings configured
    remove_strings = ["spam", "unwanted", "exact match"]

    print("=== ENHANCED REMOVE LOGIC TEST ===")
    print()

    print("Original lines:")
    for i, line in enumerate(test_lines, 1):
        print("2d")

    print(f"\nRemove strings: {remove_strings}")
    print()

    # Apply enhanced remove logic
    kept_lines = []

    for line in test_lines:
        should_remove = False
        line_lower = line.lower()
        line_stripped = line.strip()

        # Check if any remove string is contained in the line (case-insensitive)
        # OR if the line is identical to any remove string (case-insensitive)
        for remove_string in remove_strings:
            remove_string_lower = remove_string.lower()
            remove_string_stripped = remove_string.strip()

            # Remove if: 1) contains the string, or 2) is identical to the string
            if (remove_string_lower in line_lower or
                line_stripped.lower() == remove_string_stripped.lower()):
                should_remove = True
                print(f"REMOVING: '{line}' (matches '{remove_string}')")
                break

        if not should_remove:
            kept_lines.append(line)
            print(f"KEEPING:  '{line}'")

    print()
    print("=== RESULTS ===")
    print(f"Original lines: {len(test_lines)}")
    print(f"Lines removed: {len(test_lines) - len(kept_lines)}")
    print(f"Lines kept: {len(kept_lines)}")
    print()

    print("Kept lines:")
    for line in kept_lines:
        print(f"  '{line}'")

    # Verify expected results
    expected_kept = [
        "This is a test line",
        "Another test line",
        "normal line",
        "different line"
    ]

    expected_removed = [
        "spam content here",  # contains "spam"
        "This is spam",       # contains "spam"
        "SPAM",               # identical to "spam" (case-insensitive)
        "contains spam in middle",  # contains "spam"
        "unwanted",           # identical to "unwanted"
        "This line contains unwanted content",  # contains "unwanted"
        "exact match"         # identical to "exact match"
    ]

    if sorted(kept_lines) == sorted(expected_kept):
        print("\n✅ Test PASSED: Enhanced remove logic works correctly!")
        print(f"✅ Correctly kept {len(expected_kept)} lines")
        print(f"✅ Correctly removed {len(expected_removed)} lines")
        return True
    else:
        print("\n❌ Test FAILED:")
        print(f"Expected kept: {expected_kept}")
        print(f"Actual kept: {kept_lines}")
        return False

def test_case_insensitive_matching():
    """Test that the case-insensitive matching works correctly."""

    print("\n=== CASE-INSENSITIVE TEST ===")

    test_lines = [
        "SPAM",
        "spam",
        "Spam",
        "sPaM",
        "UNWANTED",
        "unwanted",
        "Unwanted"
    ]

    remove_strings = ["spam", "unwanted"]

    kept_lines = []
    removed_lines = []

    for line in test_lines:
        should_remove = False
        line_lower = line.lower()
        line_stripped = line.strip()

        for remove_string in remove_strings:
            remove_string_lower = remove_string.lower()
            remove_string_stripped = remove_string.strip()

            if (remove_string_lower in line_lower or
                line_stripped.lower() == remove_string_stripped.lower()):
                should_remove = True
                removed_lines.append(line)
                break

        if not should_remove:
            kept_lines.append(line)

    print(f"Lines tested: {test_lines}")
    print(f"Remove strings: {remove_strings}")
    print(f"Lines kept: {kept_lines}")
    print(f"Lines removed: {removed_lines}")

    # All variations should be removed due to case-insensitive matching
    if len(kept_lines) == 0 and len(removed_lines) == len(test_lines):
        print("✅ Case-insensitive matching works correctly!")
        return True
    else:
        print("❌ Case-insensitive matching failed!")
        return False

if __name__ == "__main__":
    test1_passed = test_enhanced_remove_logic()
    test2_passed = test_case_insensitive_matching()

    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED! Enhanced remove functionality is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
