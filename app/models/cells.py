import re
from dataclasses import dataclass

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

    def extract_columns_simple(self):
        sql = self.sql.decode("utf-8")
        return extract_columns(sql)

    def get_column_index(self, column_name):
        columns = self.extract_columns_simple()
        for i, (name, _) in enumerate(columns):
            if name == column_name:
                return i
        return None
