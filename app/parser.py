import io
import os
import struct
from itertools import pairwise
from operator import attrgetter
from os import PathLike

__all__ = ["SqliteParser"]

from app.models import DbHeader, LeafPageHeader, Cell


class SqliteParser:

    def __init__(self, db_path: PathLike):
        self.file_object = db_path.open("rb")  # noqa
        self.db_header = None
        self.page_header = None
        self.cells = []

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
                return self.sql(command)

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

    def decode_cell(self, buffer):
        record_size = self.get_varint(buffer)
        row_id = self.get_varint(buffer)
        record_header_size = self.get_varint(buffer)
        st_schema_type = self.get_varint(buffer)
        st_schema_name = self.get_varint(buffer)
        st_schema_tbl_name = self.get_varint(buffer)
        st_schema_root_page = self.get_varint(buffer)
        st_schema_sql = self.get_varint(buffer)
        type_ = buffer.read(self.get_serial_type_code(st_schema_type))
        name = buffer.read(self.get_serial_type_code(st_schema_name))
        tbl_name = buffer.read(self.get_serial_type_code(st_schema_tbl_name)).decode(
            "utf-8"
        )
        root_page = int.from_bytes(
            buffer.read(self.get_serial_type_code(st_schema_root_page))
        )
        sql = buffer.read(self.get_serial_type_code(st_schema_sql))
        cell = Cell(
            record_size=record_size,
            row_id=row_id,
            header_size=record_header_size,
            type=type_,
            name=name,
            tbl_name=tbl_name,
            root_page=root_page,
            sql=sql,
        )
        return cell

    def get_cells(self):
        self.db_header = db_header = DbHeader.from_bytes(self.file_object)
        self.page_header = page_header = LeafPageHeader.from_bytes(self.file_object)
        page_size = db_header.page_size
        cell_count = page_header.cell_count

        unpacked_offsets = struct.unpack(
            f">{cell_count}H", self.file_object.read(cell_count * 2)
        )
        offsets = sorted((page_size, *unpacked_offsets))

        for start, stop in pairwise(offsets):
            self.file_object.seek(start, os.SEEK_SET)
            cell = self.file_object.read(stop - start)
            self.cells.append(self.decode_cell(io.BytesIO(cell)))

    def db_info(self):
        self.db_header = db_header = DbHeader.from_bytes(self.file_object)
        self.page_header = page_header = LeafPageHeader.from_bytes(self.file_object)
        print("database page size: ", db_header.page_size)
        print("number of tables: ", page_header.cell_count)
        return db_header, page_header


    def tables(self):
        self.get_cells()
        print(" ".join(sorted(map(attrgetter("tbl_name"), self.cells), key=str.lower)))  # noqa
        return

    def sql(self, command):
        self.get_cells()
        *_, table_name = command.split()
        cell = next(c for c in self.cells if c.tbl_name == table_name)
        self.file_object.seek(self.db_header.page_size * (cell.root_page - 1), os.SEEK_SET)
        _page_number = struct.unpack(">HH", self.file_object.read(4))[0]
        row_id = self.file_object.read(1)[0]
        print(row_id)
