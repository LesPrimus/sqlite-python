import pathlib

from app.models import DbHeader, LeafPageHeader
from app.models.tables import Page


def test_tables_exist():
    path = pathlib.Path(__file__).parent.parent.parent / 'sample.db'
    with path.open('rb') as f:
        db_header = DbHeader.from_bytes(f)
        page_header = LeafPageHeader.from_bytes(f)
        root = Page(db_header=db_header, page_header=page_header)
        root.decode_cells(f)
    assert 0