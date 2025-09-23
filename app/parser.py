import io
import os
import struct
from itertools import pairwise
from operator import attrgetter
from os import PathLike

__all__ = ["SqliteParser"]

from app.models import DbHeader, LeafPageHeader, Cell, Record
from app.models.tables import SchemaTable


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

    def get_schema_table(self):
        db_header = DbHeader.from_bytes(self.file_object)
        page_header = LeafPageHeader.from_bytes(self.file_object)
        page_size = db_header.page_size
        cell_count = page_header.cell_count

        unpacked_offsets = struct.unpack(
            f">{cell_count}H", self.file_object.read(cell_count * 2)
        )
        offsets = sorted((page_size, *unpacked_offsets))

        cells = []
        for start, stop in pairwise(offsets):
            self.file_object.seek(start, os.SEEK_SET)
            cell = self.file_object.read(stop - start)
            cells.append(self.decode_cell(io.BytesIO(cell)))
        return SchemaTable(db_header, page_header, cells)

    def decode_record(self, buffer: io.BytesIO, columns: dict[str, str]) -> Record:
        record_size = self.get_varint(buffer)
        row_id = self.get_varint(buffer)

        # Read header size
        _header_size = self.get_varint(buffer)

        # Read serial type codes
        serial_types = [self.get_varint(buffer) for _ in range(len(columns))]

        # Now read the actual data
        values = []
        for serial_type in serial_types:
            value = self.decode_value_by_serial_type(buffer, serial_type)
            values.append(value)

        return Record(record_size=record_size, row_id=row_id, values=values)

    def get_records(self, db_header: DbHeader, root_cell: Cell) -> list[Record]:
        page_header = LeafPageHeader.from_bytes(self.file_object)
        page_size = db_header.page_size

        cell_count = page_header.cell_count

        unpacked_offsets = struct.unpack(
            f">{cell_count}H", self.file_object.read(cell_count * 2)
        )
        offsets = sorted(
            [page_size * root_cell.root_page]
            + [off + page_size for off in unpacked_offsets]
        )

        records = []
        for start, stop in pairwise(offsets):
            self.file_object.seek(start, os.SEEK_SET)
            cell_data = self.file_object.read(stop - start)
            buffer = io.BytesIO(cell_data)

            try:
                record = self.decode_record(buffer, columns=root_cell.columns)
                records.append(record)
            except Exception as e:
                print(f"Error decoding record at offset {start}: {e}")
                continue

        return records

    def db_info(self):
        self.db_header = db_header = DbHeader.from_bytes(self.file_object)
        self.page_header = page_header = LeafPageHeader.from_bytes(self.file_object)
        print("database page size: ", db_header.page_size)
        print("number of tables: ", page_header.cell_count)
        return db_header, page_header

    def tables(self):
        schema_table = self.get_schema_table()
        print(
            " ".join(
                sorted(map(attrgetter("tbl_name"), schema_table.cells), key=str.lower)  # noqa
            )
        )  # noqa
        return

    def count_rows(self, table_name):
        schema_table = self.get_schema_table()
        [cell] = [c for c in schema_table.cells if c.tbl_name == table_name]
        records = self.get_records(schema_table.db_header, cell)
        print(len(records))
        return len(records)

    def fetch_from_table(self, name, table_name):
        name = name.lower().strip()
        schema_table = self.get_schema_table()
        [cell] = [c for c in schema_table.cells if c.tbl_name == table_name]
        _index = cell.get_column_index(name)
        records = self.get_records(schema_table.db_header, cell)
        ret = [record.values[_index] for record in records]
        print("\n".join(map(str, ret)))
        return ret

    def sql(self, command):
        _, operation, _, table_name = command.split()
        match operation.lower():
            case "count(*)":
                return self.count_rows(table_name)
            case str() as column_name:
                return self.fetch_from_table(column_name, table_name)
            case _:
                raise ValueError(f"Unsupported SQL operation: {operation}")
