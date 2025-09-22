import pathlib

from app.parser import SqliteParser


class TestParser:
    def test_db_file_content(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        parser.get_cells()
        # print(parser.cells)
        print(parser.db_header)
        print(parser.page_header)
        assert 0
