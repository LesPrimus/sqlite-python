import pathlib

from app.parser import SqliteParser


class TestParser:
    def test_parser_count_rows(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT COUNT(*) FROM movie") # noqa
        assert expected == 2

    def test_parser_fetch_from_table(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT title FROM movie") # noqa
        assert sorted(expected) == sorted(
            [
                "Monty Python and the Holy Grail",
                "And Now for Something Completely Different",
            ]
        )
