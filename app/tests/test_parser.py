import pathlib
from operator import itemgetter

from app.parser import SqliteParser


class TestParser:
    def test_print_number_of_tables(self):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.handle_command(".dbinfo")
        assert expected == (4096, 3)

    def test_print_table_name(self):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.handle_command(".tables")
        assert expected == ["apples", "oranges"]

    def test_count_rows(self):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT count(*) FROM apples")  # noqa
        assert expected == 4

    def test_read_data_from_a_single_column(self, db_file):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT name FROM apples")  # noqa
        assert sorted(expected) == sorted(
            [("Fuji",), ("Golden Delicious",), ("Granny Smith",), ("Honeycrisp",)]
        )

    def test_read_data_from_multiple_columns(self, db_file):
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

    def test_filter_data_with_a_where_clause(self, db_file):
        path = pathlib.Path("sample.db")
        parser = SqliteParser(path)
        expected = parser.sql("SELECT name, color FROM apples WHERE color = 'Yellow'")  # noqa

        assert expected == [("Golden Delicious", "Yellow")]

    def test_retrieve_data_using_a_full_table_scan(self):
        path = pathlib.Path("superheroes.db")
        parser = SqliteParser(path)
        expected = parser.sql(
            "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"  # noqa
        )  # noqa
        assert expected == [
            (297, "Stealth (New Earth)"),
            (790, "Tobias Whale (New Earth)"),
            (1085, "Felicity (New Earth)"),
            (2729, "Thrust (New Earth)"),
            (3289, "Angora Lapin (New Earth)"),
            (3913, "Matris Ater Clementia (New Earth)"),
        ]

    # def test_retrieve_data_using_an_index(self):
    #     path = pathlib.Path("companies.db")
    #     parser = SqliteParser(path)
    #     expected = parser.sql("SELECT id, name FROM companies WHERE country = 'eritrea'")  # noqa
    #     assert 0
