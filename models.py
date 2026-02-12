"""Data models for the prompt cleaning workflow."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class Item:
    """Represents an item flowing through the workflow.

    Attributes:
        raw: The original, unprocessed string (temporary, not persisted).
        prompt: The cleaned string returned from the LLM (the persistent ID).
    """

    raw: str
    prompt: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Convert the item to a dictionary for persistence."""
        return {
            "prompt": self.prompt,
        }
