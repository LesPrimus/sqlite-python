from dataclasses import dataclass

__all__ = ["Cell"]


@dataclass
class Cell:
    record_size: int
    row_id: int
    header_size: int
    type: str
    name: str
    tbl_name: str
    root_page: int
    sql: bytes
