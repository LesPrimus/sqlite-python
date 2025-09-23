from dataclasses import dataclass, field

__all__ = ["Record"]

from typing import Any


@dataclass
class Record:
    record_size: int
    row_id: int
    values: list[Any] = field(default_factory=list)
