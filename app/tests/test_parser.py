import pathlib
from operator import itemgetter

from app.parser import SqliteParser


class TestParser:
    def test_decode_cells(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        [cell] = parser.schema_table.cells
        assert cell.columns == {"title": "VARCHAR", "year": "INT", "score": "INT"}

    def test_parser_count_rows(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT COUNT(*) FROM movie")  # noqa
        assert expected == 2

    def test_parser_fetch_from_table(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT title FROM movie")  # noqa
        assert sorted(expected) == sorted(
            [
                ("Monty Python and the Holy Grail",),
                ("And Now for Something Completely Different",),
            ]
        )

    def test_parser_fetch_from_tables(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT title, year FROM movie")  # noqa
        assert sorted(expected, key=itemgetter(0)) == sorted(
            [
                ("And Now for Something Completely Different", 1971),
                ("Monty Python and the Holy Grail", 1975),
            ],
            key=itemgetter(0),
        )
