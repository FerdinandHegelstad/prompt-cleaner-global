#!normalizer.py
import re

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