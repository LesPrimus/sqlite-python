from dataclasses import dataclass, field

__all__ = ["Cell", "LeafCell"]

from app.utils import extract_columns


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
    columns: dict[str, str] = field(init=False)

    def __post_init__(self):
        self.columns = self.extract_columns_simple()

    def extract_columns_simple(self):
        sql = self.sql.decode("utf-8")
        return extract_columns(sql)

    def get_column_index(self, column_name):
        for i, name in enumerate(self.columns):
            if name == column_name:
                return i
        return None

@dataclass
class LeafCell:
    payload_size: int
    row_id: int
    payload: list[bytes] = field(default_factory=list)
