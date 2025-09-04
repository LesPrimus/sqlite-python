import os
import struct
from os import PathLike

__all__ = ["SqliteParser"]


class SqliteParser:
    AVAILABLE_COMMANDS = [".dbinfo"]

    def __init__(self, db_path: PathLike):
        self.db_path = db_path

    def handle_command(self, command: str):
        match command:
            case ".dbinfo":
                return self.db_info()
            case _:
                raise ValueError(f"Invalid command: {command}")

    def db_info(self):
        with self.db_path.open("rb") as db_file:  # noqa
            db_file.seek(16)
            page_size = struct.unpack(">H", db_file.read(2))[0]
            print("database page size: ", page_size)
            db_file.seek(103, os.SEEK_SET)
            page_count = struct.unpack(">H", db_file.read(2))[0]
            print("number of tables: ", page_count)
