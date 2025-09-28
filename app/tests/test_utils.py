import pytest

from app.utils import ParsedCommand, parse_command


@pytest.mark.parametrize(
    "command, expected",
    [
        (
            "SELECT COUNT(*) FROM movie",
            ParsedCommand(table_name="movie", function="COUNT", columns=[], where=None),
        ),
        (
            "SELECT title, year FROM movie",
            ParsedCommand(
                table_name="movie", function=None, columns=["title", "year"], where=None
            ),
        ),
        (
            "SELECT title FROM movie",
            ParsedCommand(
                table_name="movie", function=None, columns=["title"], where=None
            ),
        ),
        (
            "SELECT title, year FROM movie WHERE year == 1975",
            ParsedCommand(
                table_name="movie",
                function=None,
                columns=["title", "year"],
                where="year == 1975",
            ),
        ),
    ],
)
def test_parse_command(command, expected):
    assert parse_command(command) == expected
