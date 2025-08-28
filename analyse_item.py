#!analyse_item.py
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Item:
    """Represents an item flowing through the workflow.

    Attributes:
        default: The original, unprocessed string.
        cleaned: The cleaned string returned from the LLM, if available.
        normalized: The normalized version of the cleaned string, if available.
        occurrences: The number of times this item has been encountered (default: 1).
    """

    default: str
    cleaned: Optional[str] = None
    normalized: Optional[str] = None
    occurrences: int = 1

    def to_dict(self) -> Dict[str, Optional[str | int]]:
        """Convert the item to a dictionary for persistence.

        Returns a dictionary containing the original, cleaned, normalized
        representations and occurrence count. Keys are aligned with expectations of database code.
        """
        return {
            "default": self.default,
            "cleaned": self.cleaned,
            "normalized": self.normalized,
            "occurrences": self.occurrences,
        }


