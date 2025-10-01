import os
from dataclasses import dataclass, field
from io import BufferedReader
from itertools import pairwise

from app.models import DbHeader, LeafPageHeader
from app.models.cells import RootCell
from app.utils import get_offsets, get_varint, get_serial_type_code


__all__ = ["RootPage"]


@dataclass(kw_only=True)
class RootPage:
    db_header: DbHeader
    page_header: LeafPageHeader
    cells: list[RootCell] = field(default_factory=list)

    @classmethod
    def from_bytes(cls, buffer: BufferedReader):
        db_header = DbHeader.from_bytes(buffer)
        page_header = LeafPageHeader.from_bytes(buffer)
        cell_count = page_header.cell_count
        page_size = db_header.page_size
        offsets = get_offsets(buffer, page_size, cell_count)

        cells = []
        for start, stop in pairwise(offsets):
            buffer.seek(start, os.SEEK_SET)
            #
            payload_size = get_varint(buffer)
            row_id = get_varint(buffer)

            # Read header size
            header_start_pos = buffer.tell()
            #
            header_size = get_varint(buffer)

            # Read serial type codes
            serial_types = []
            while buffer.tell() < header_start_pos + header_size:
                serial_types.append(get_varint(buffer))

            values = []
            for serial_type in serial_types:
                to_read = get_serial_type_code(serial_type)
                values.append(buffer.read(to_read))
            schema_type, schema_name, schema_tbl_name, schema_root_page, schema_sql = (
                values
            )
            schema_root_page = int.from_bytes(schema_root_page, "big")
            cell = RootCell(
                record_size=payload_size,
                row_id=row_id,
                header_size=header_size,
                type=schema_type,
                name=schema_name,
                tbl_name=schema_tbl_name,
                root_page=schema_root_page,
                sql=schema_sql,
            )
            cells.append(cell)
        return cls(db_header=db_header, page_header=page_header, cells=cells)
