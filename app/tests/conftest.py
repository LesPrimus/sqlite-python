import pathlib
import sqlite3
from tempfile import NamedTemporaryFile

import pytest



@pytest.fixture(scope="function")
def db_file():
    temp_file = NamedTemporaryFile(delete=False)
    try:
        # Create a connection and add some data
        with sqlite3.connect(temp_file.name) as conn:
            cursor = conn.cursor()
            cursor.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
            conn.commit()
        yield temp_file
    finally:
        # Clean up the file after the test
        temp_file.close()
        pathlib.Path(temp_file.name).unlink(missing_ok=True)
