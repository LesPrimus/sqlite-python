import io
import os
import struct
from functools import cached_property
from itertools import pairwise
from operator import attrgetter
from os import PathLike

__all__ = ["SqliteParser"]

from app.models import DbHeader, LeafPageHeader, Cell, Record
from app.models.tables import SchemaTable
from app.utils import parse_command


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
                return self.db_info(verbose=True)
            case ".tables":
                return self.tables(verbose=True)
            case _:
                return self.sql(command)

    @classmethod
    def decode_value_by_serial_type(cls, buffer, serial_type: int):
        """Decode a value from buffer based on its serial type."""
        match serial_type:
            case 0:
                return None  # NULL
            case 1:
                return struct.unpack(">b", buffer.read(1))[0]  # 8-bit signed int
            case 2:
                return struct.unpack(">h", buffer.read(2))[0]  # 16-bit signed int
            case 3:
                # 24-bit signed integer
                data = buffer.read(3)
                value = int.from_bytes(data, "big", signed=True)
                return value
            case 4:
                return struct.unpack(">i", buffer.read(4))[0]  # 32-bit signed int
            case 5:
                # 48-bit signed integer
                data = buffer.read(6)
                value = int.from_bytes(data, "big", signed=True)
                return value
            case 6:
                return struct.unpack(">q", buffer.read(8))[0]  # 64-bit signed int
            case 7:
                return struct.unpack(">d", buffer.read(8))[0]  # 64-bit float
            case 8:
                return 0  # Integer constant 0
            case 9:
                return 1  # Integer constant 1
            case _ if serial_type >= 12 and serial_type % 2 == 0:
                # BLOB
                length = (serial_type - 12) // 2
                return buffer.read(length)
            case _ if serial_type >= 13 and serial_type % 2 != 0:
                # TEXT
                length = (serial_type - 13) // 2
                return buffer.read(length).decode("utf-8")
            case _:
                raise ValueError(f"Invalid serial type code: {serial_type}")

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

    @cached_property
    def schema_table(self):
        db_header = DbHeader.from_bytes(self.file_object)
        page_header = LeafPageHeader.from_bytes(self.file_object)
        page_size = db_header.page_size
        cell_count = page_header.cell_count

        unpacked_offsets = struct.unpack(
            f">{cell_count}H", self.file_object.read(cell_count * 2)
        )
        offsets = list(sorted((page_size, *unpacked_offsets)))

        cells = []
        for start, stop in pairwise(offsets):
            self.file_object.seek(start, os.SEEK_SET)
            cell = self.file_object.read(stop - start)
            cells.append(self.decode_cell(io.BytesIO(cell)))
        return SchemaTable(db_header, page_header, cells)

    def decode_record(self, buffer: io.BytesIO) -> Record:
        record_size = self.get_varint(buffer)
        row_id = self.get_varint(buffer)

        header_start = buffer.tell()
        # Read header size
        header_size = self.get_varint(buffer)

        # Read serial type codes
        serial_types = []
        while buffer.tell() < header_start + header_size:
            serial_types.append(self.get_varint(buffer))

        # Now read the actual data
        values = []
        for serial_type in serial_types:
            value = self.decode_value_by_serial_type(buffer, serial_type)
            values.append(value)

        return Record(record_size=record_size, row_id=row_id, values=values)

    def read_page(self, page_number: int, page_size: int = 4096):
        self.file_object.seek(page_size * (page_number - 1), os.SEEK_SET)
        page_header = LeafPageHeader.from_bytes(self.file_object)
        offsets = [
            int.from_bytes(self.file_object.read(2), "big")
            for _ in range(page_header.cell_count)
        ]

        records = []
        for offset in sorted(offsets):
            self.file_object.seek(offset + (page_size * (page_number - 1)), os.SEEK_SET)
            record = self.decode_record(self.file_object)
            records.append(record)
        return records

    def get_records(self, db_header: DbHeader, root_cell: Cell) -> list[Record]:
        page_size = db_header.page_size
        page_header = LeafPageHeader.from_bytes(self.file_object)
        if page_header.page_type == 5:  # interior page
            _right_most_pointer = int.from_bytes(self.file_object.read(4), "big")
        cell_count = page_header.cell_count

        offsets = [
            int.from_bytes(self.file_object.read(2), "big") for _ in range(cell_count)
        ]
        if page_header.page_type == 5:
            interior_page_cells = []
            for offset in offsets:
                self.file_object.seek(offset + page_size, os.SEEK_SET)
                page_number = int.from_bytes(self.file_object.read(4), "big")
                row_id = self.get_varint(self.file_object)
                interior_page_cells.append((page_number, row_id))

            records = []
            for page_number, row_id in interior_page_cells:
                records.extend(self.read_page(page_number))
            return records
        else:
            records = self.read_page(root_cell.root_page)
            return records

    def db_info(self, verbose=False):
        self.db_header = db_header = DbHeader.from_bytes(self.file_object)
        self.page_header = page_header = LeafPageHeader.from_bytes(self.file_object)
        if verbose:
            print("database page size: ", db_header.page_size)
            print("number of tables: ", page_header.cell_count)
        return db_header.page_size, page_header.cell_count

    def tables(self, verbose=False):
        schema_table = self.schema_table
        tables = sorted(
            cell.tbl_name
            for cell in schema_table.cells
            if cell.tbl_name != "sqlite_sequence"
        )
        if verbose:
            print(" ".join(tables))
        return tables

    def count_rows(self, table_name, *, verbose=False):
        schema_table = self.schema_table
        [cell] = [c for c in schema_table.cells if c.tbl_name == table_name]
        records = self.get_records(schema_table.db_header, cell)
        if verbose:
            print(len(records))
        return len(records)

    def get_cell(self, table_name):
        schema_table = self.schema_table
        [cell] = [c for c in schema_table.cells if c.tbl_name == table_name]
        return cell

    @classmethod
    def filter_records(
        cls, *records: Record, cell: Cell, sep: str = "=", where: str | None = None
    ):
        if where:
            column, param = map(str.strip, where.split(sep))
            param = param.strip("'")
            # _index = cell.get_column_index(column)
            filtered_records = [
                record for record in records if param in set(record.values)
            ]
            return filtered_records
        else:
            return records

    def fetch_columns(self, *columns, table_name, where, verbose=False):
        schema_table = self.schema_table
        cell = self.get_cell(table_name)
        records = self.get_records(schema_table.db_header, cell)
        records = self.filter_records(*records, cell=cell, where=where)
        results = []
        for record in records:
            entry = []
            for column in columns:
                if column == "id":
                    entry.append(record.row_id)
                else:
                    idx = cell.get_column_index(column)
                    entry.append(record.values[idx])
            results.append(tuple(entry))
        if verbose:
            for result in results:
                print("|".join(map(str, result)))
        return results

    def sql(self, command):
        command = parse_command(command)
        if command.function == "count":
            return self.count_rows(command.table_name, verbose=True)
        else:
            print(command)
            return self.fetch_columns(
                *command.columns,
                table_name=command.table_name,
                where=command.where,
                verbose=True,
            )
