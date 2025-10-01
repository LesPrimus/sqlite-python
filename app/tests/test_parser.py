import pathlib
from operator import itemgetter

import pytest

from app.parser import SqliteParser


class TestParser:
    @pytest.mark.parametrize("command", [".dbinfo"])
    def test_print_page_size(self, command):
        path = pathlib.Path("sample.db")
        with SqliteParser(path) as parser:
            expected = parser.handle_command(command)
        assert expected == [
            f"database page size: {parser.root_page.db_header.page_size}",
            f"number of tables: {len(parser.root_page.cells)}",
        ]

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

    def test_parser_filter(self, db_file):
        path = pathlib.Path(db_file.name)
        parser = SqliteParser(path)
        expected = parser.sql("SELECT title, year FROM movie WHERE year == 1975")

        assert expected == [("Monty Python and the Holy Grail", 1975)]
        assert 0, "Finish the test."
