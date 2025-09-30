import os
from dataclasses import dataclass, field
from io import BufferedReader
from itertools import pairwise

from app.models import DbHeader, LeafPageHeader, Cell

__all__ = ["SchemaTable"]

from app.models.cells import LeafCell

from app.utils import get_offsets, get_varint, get_serial_type_code


@dataclass(kw_only=True)
class Page:
    page_header: LeafPageHeader
    db_header: DbHeader = field(default=None)
    cells: list[Cell] = field(default_factory=list)

    def decode_cells(self, buffer: BufferedReader):
        page_type = self.page_header.page_type
        if page_type == 13:
            self.decode_leaf_page_cells(buffer)

    def decode_leaf_page_cells(self, buffer: BufferedReader):
        cells = []
        cell_count = self.page_header.cell_count
        page_size = self.db_header.page_size
        offsets = get_offsets(buffer, page_size, cell_count)

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

            cell = LeafCell(payload_size, row_id)
            for serial_type in serial_types:
                to_read = get_serial_type_code(serial_type)
                cell.payload.append(buffer.read(to_read))
            cells.append(cell)
        return cells

class SchemaTable(Page):
    db_header: DbHeader
    page_header: LeafPageHeader
    cells: list[Cell] = field(default_factory=list)
