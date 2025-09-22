import pathlib

from app.models import LeafPageHeader
from app.parser import SqliteParser


class TestParser:
    def test_db_file_content(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        schema_table = parser.get_schema_table()
        [cell] = schema_table.cells
        records = parser.get_records(schema_table.db_header, cell.root_page)
        print(records)
        assert 0
