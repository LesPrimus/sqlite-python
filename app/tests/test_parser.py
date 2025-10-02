import pathlib
from operator import itemgetter

from app.parser import SqliteParser


class TestParser:
    def test_decode_cells(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        [cell] = parser.schema_table.cells
        assert cell.columns == {"title": "VARCHAR", "year": "INT", "score": "INT"}

    def test_parser_count_rows(self):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT count(*) FROM apples")  # noqa
        assert expected == 4

    def test_parser_fetch_from_table(self, db_file):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT name FROM apples")  # noqa
        assert sorted(expected) == sorted(
            [("Fuji",), ("Golden Delicious",), ("Granny Smith",), ("Honeycrisp",)]
        )

    def test_parser_fetch_from_tables(self, db_file):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT name, color FROM apples")  # noqa
        assert sorted(expected, key=itemgetter(0)) == sorted(
            [
                ("Fuji", "Red"),
                ("Golden Delicious", "Yellow"),
                ("Granny Smith", "Light Green"),
                ("Honeycrisp", "Blush Red"),
            ],
            key=itemgetter(0),
        )

    def test_parser_filter(self, db_file):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT name, color FROM apples WHERE color = 'Yellow'") # noqa

        assert expected == [('Golden Delicious', 'Yellow')]

    def test_retrieve_data_using_a_full_table_scan(self):
        path = pathlib.Path("superheroes.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'") # noqa
        assert 0