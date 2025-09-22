from dataclasses import dataclass, field

from app.models import DbHeader, LeafPageHeader, Cell

__all__ = ["SchemaTable"]

@dataclass
class SchemaTable:
    db_header: DbHeader
    page_header: LeafPageHeader
    cells: list[Cell] = field(default_factory=list)
