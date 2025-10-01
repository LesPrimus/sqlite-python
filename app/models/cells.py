from dataclasses import dataclass, field

__all__ = ["LeafCell", "RootCell"]

from app.utils import extract_columns


@dataclass(kw_only=True)
class RootCell:
    record_size: int
    row_id: int
    header_size: int
    type: str
    name: str
    tbl_name: str
    root_page: int
    sql: bytes

    columns: dict[str, str] = field(init=False)

    def __post_init__(self):
        self.columns = extract_columns(self.sql.decode("utf-8"))


@dataclass
class LeafCell:
    payload_size: int
    row_id: int
    payload: list[bytes] = field(default_factory=list)
