from dataclasses import dataclass

__all__ = ["Record"]


@dataclass
class Record:
    payload_size: int
    row_id: int
    payload: bytes

