import re
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

    def extract_columns_simple(self):
        sql = self.sql.decode("utf-8")
        pattern = r'CREATE\s+TABLE\s+\w+\s*\(\s*([^)]+)\s*\)'
        match = re.search(pattern, sql, re.IGNORECASE)

        if match:
            columns_str = match.group(1)
            # Split by comma and clean up
            columns = [col.strip() for col in columns_str.split(',')]
            return columns
        return []
