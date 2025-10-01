import pathlib
from app.models.tables import RootPage


def test_tables_exist():
    path = pathlib.Path(__file__).parent.parent.parent / "sample.db"
    with path.open("rb") as f:
        root_page = RootPage.from_bytes(f)
        sql = "SELECT name FROM apples"

    assert 0
