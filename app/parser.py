import io
import os
import struct
from itertools import pairwise
from os import PathLike

__all__ = ["SqliteParser"]



class SqliteParser:
    AVAILABLE_COMMANDS = [".dbinfo", ".tables",]

    def __init__(self, db_path: PathLike):
        self.file_object = db_path.open("rb") # noqa
        self._tables_names = []

    def __enter__(self):
        self.file_object.__enter__()
        return self

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
        _record_header_size = self.get_varint(buffer)
        st_schema_type = self.get_varint(buffer)
        st_schema_name = self.get_varint(buffer)
        st_schema_tbl_name = self.get_varint(buffer)
        _st_schema_root_page = self.get_varint(buffer)
        _st_schema_sql = self.get_varint(buffer)
        buffer.read(self.get_serial_type_code(st_schema_type))
        buffer.read(self.get_serial_type_code(st_schema_name))
        self._tables_names.append(buffer.read(self.get_serial_type_code(st_schema_tbl_name)).decode("utf-8")) # <===

    def decode_cell(self, buffer):
        _record_size = self.get_varint(buffer)
        _row_id = self.get_varint(buffer)
        self.decode_record(buffer)

    def db_info(self):
        self.file_object.seek(16)
        page_size = struct.unpack(">H", self.file_object.read(2))[0]
        print("database page size: ", page_size)
        self.file_object.seek(103, os.SEEK_SET)
        page_count = struct.unpack(">H", self.file_object.read(2))[0]
        print("number of tables: ", page_count)

    def tables(self):

        # -- file header
        self. file_object.read(16)
        page_size = struct.unpack(">H", self.file_object.read(2))[0]
        self.file_object.read(82)

        # -- page header
        page_type, _, nr_cells, start_cell_content_area, _  = struct.unpack(">bHHHb", self.file_object.read(8))

        offsets = list(struct.unpack(f">{nr_cells}H", self.file_object.read(nr_cells * 2))) + [page_size]

        for start, stop in pairwise(sorted(offsets)):
            self.file_object.seek(start, os.SEEK_SET)
            cell = self.file_object.read(stop - start)
            self.decode_cell(io.BytesIO(cell))
        print(" ".join(sorted(self._tables_names)))
