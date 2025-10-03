from dataclasses import dataclass, field

__all__ = ["Cell"]

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
    columns: list[str] = field(init=False)

    def __post_init__(self):
        self.columns = self.extract_columns_simple()

    def extract_columns_simple(self):
        sql = self.sql.decode("utf-8")
        return extract_columns(sql)

    def get_column_index(self, column_name):
        for i, column in enumerate(self.columns):
            if column == column_name:
                return i
        raise ValueError(f"Column {column_name} not found in table {self.tbl_name}")
