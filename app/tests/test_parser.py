import pathlib

from app.models import LeafPageHeader
from app.parser import SqliteParser


class TestParser:
    def test_parser_count_rows(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT COUNT(*) FROM movie")
        assert expected == 2

    def test_parser_fetch_from_table(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT title FROM movie")
        assert 0
        assert expected == ["The Shawshank Redemption", "The Godfather"]