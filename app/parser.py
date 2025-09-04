import os
import struct
from os import PathLike

__all__ = ["SqliteParser"]


class SqliteParser:
    AVAILABLE_COMMANDS = [".dbinfo", ".tables",]

    def __init__(self, db_path: PathLike):
        self.file_object = db_path.open("rb")

    def __enter__(self):
        self.file_object.__enter__()
        return self.file_object

    def __exit__(self, *args):
        return self.file_object.__exit__(*args)

    def handle_command(self, command: str):
        match command:
            case ".dbinfo":
                return self.db_info()
            case ".tables":
                return self.tables()
            case _:
                raise ValueError(f"Invalid command: {command}")

    def db_info(self):
        with self as db_file:  # noqa
            db_file.seek(16)
            page_size = struct.unpack(">H", db_file.read(2))[0]
            print("database page size: ", page_size)
            db_file.seek(103, os.SEEK_SET)
            page_count = struct.unpack(">H", db_file.read(2))[0]
            print("number of tables: ", page_count)

    def tables(self):
        with self as db_file:
            db_file.seek(100, os.SEEK_SET) #skip db file header
            # -- page_header
            page_type, _, nr_cells, start_cell_content_area, _  = struct.unpack(">bHHHb", db_file.read(8))
            print(page_type, nr_cells, start_cell_content_area)

            offsets = sorted(struct.unpack(f">{nr_cells}H", db_file.read(nr_cells * 2)))
            print(offsets)

            # page_type = struct.unpack(">b", db_file.read(1))[0]
            # free_block_start = struct.unpack(">H", db_file.read(2))[0]
            # number_of_cells = struct.unpack(">H", db_file.read(2))[0]
            # start_of_cell_content_area = struct.unpack(">H", db_file.read(2))[0]
            # number_of_fragments = struct.unpack(">b", db_file.read(1))[0]
            # right_most_pointer = struct.unpack(">HH", db_file.read(4))[0]
            # print(page_type)
            # print(free_block_start)
            # print(number_of_cells)
            # print(start_of_cell_content_area)
            # print(number_of_fragments)
            # print(right_most_pointer)
            # page_header =
            # [cell_count] = struct.unpack(">H", db_file.read(2))
            # offsets = struct.unpack(f">{cell_count}H", db_file.read(cell_count * 2))
            # start_cell_content_area =