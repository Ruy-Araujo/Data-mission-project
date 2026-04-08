from __future__ import annotations

from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """Base contract for all extractors in this repository."""

    @abstractmethod
    def run(self) -> str:
        """Execute extraction and return the uploaded object key."""
        raise NotImplementedError
