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

    @classmethod
    def get_varint(cls, buffer):
        result = 0
        while True:
            value = int.from_bytes(buffer.read(1), "big")
            result = (result << 7) | (value & 0x7F)
            if (value & 0x80) == 0:
                break
        return result

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

    @classmethod
    def get_serial_type_code(cls, n: int) -> int | str:
        match n:
            case 0:
                return "NULL"
            case 1:
                return 1
            case 2:
                return 2
            case 3:
                return 3
            case 4:
                return 4
            case 5:
                return 6
            case 6:
                return 8
            case 7:
                return 8
            case 8:
                return 0
            case 9:
                return 0
            case 10 | 11:
                return "VARIABLE"
            case _ if n >= 12 and n % 2 == 0:
                return (n - 12) // 2
            case _ if n >= 13 and n % 2 != 0:
                return (n - 13) // 2
            case _:
                raise ValueError(f"Invalid serial type code: {n}")

    def decode_record(self, buffer):
        print("---- RECORD HEADER----")
        record_header_size = self.get_varint(buffer)
        schema_type_size = self.get_serial_type_code(self.get_varint(buffer))
        print(schema_type_size)


    def tables(self):
        with self as db_file:
            # -- file header
            db_file.read(16)
            page_size = struct.unpack(">H", db_file.read(2))[0]
            db_file.read(82)

            # -- page header
            page_type, _, nr_cells, start_cell_content_area, _  = struct.unpack(">bHHHb", db_file.read(8))

            db_file.seek(start_cell_content_area, os.SEEK_SET)

            # --- cell area
            for _ in range(nr_cells):
                record_size = self.get_varint(db_file)
                row_id = self.get_varint(db_file)
                print(record_size)
                print(row_id)
                self.decode_record(db_file)
                break