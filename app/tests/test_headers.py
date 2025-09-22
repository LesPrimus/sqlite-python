from app.models import DbHeader, LeafPageHeader

def test_db_header(db_file):
    header = DbHeader.from_bytes(db_file)
    assert header.magic_header_str == "SQLite format 3\x00"
    assert header.page_size == 4096
    assert header.file_format_write_version_number == 1
    assert header.file_format_read_version_number == 1
    assert header.reserved_bytes_per_pages == 0
    assert header.max_embedded_payload_fraction == 64
    assert header.min_embedded_payload_fraction == 32
    assert header.leaf_payload_fraction == 32
    assert header.file_change_counter == 1
    assert header.size_in_pages == 2
    assert header.freelist_trunk_page_number == 0
    assert header.freelist_pages_number == 0
    assert header.schema_cookie == 1
    assert header.schema_format_number == 4
    assert header.default_page_cache_size == 0
    assert header.largest_root_page_number == 0
    assert header.db_text_encoding == 1
    assert header.user_version == 0
    assert header.incremental_vacuum_mode == 0
    assert header.application_id == 0
    assert header.reserved_bytes_per_page == 0
    assert header.version_valid_for == 1
    assert header.sqlite_version_number == 3046000

def test_leaf_page_header(db_file):
    _ = DbHeader.from_bytes(db_file)
    header = LeafPageHeader.from_bytes(db_file)
    assert header.page_type == 13
    assert header.first_free_block == 0
    assert header.cell_count == 1
    assert header.cell_content_area == 4032
    assert header.fragment_free_bytes == 0
