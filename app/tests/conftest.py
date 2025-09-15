import pathlib

import pytest


@pytest.fixture(scope="function")
def db_file():
    with pathlib.Path("chinook.db").open("rb") as f:
        yield f
