from app.models import DbHeader


def test_db_header(db_file):
    header = DbHeader.from_bytes(db_file)
    assert header.magic_header_str == "SQLite format 3\x00"
    assert header.page_size == 1024
    assert header.file_format_write_version_number == 1
    assert header.file_format_read_version_number == 1
    assert header.reserved_bytes_per_pages == 0
    assert header.max_embedded_payload_fraction == 64
    assert header.min_embedded_payload_fraction == 32
    assert header.leaf_payload_fraction == 32
    assert header.file_change_counter == 25
    assert header.size_in_pages == 864
    assert header.freelist_trunk_page_number == 0
    assert header.freelist_pages_number == 0
    assert header.schema_cookie == 34
    assert header.schema_format_number == 1
    assert header.default_page_cache_size == 0
    assert header.largest_root_page_number == 0
    assert header.db_text_encoding == 1
    assert header.user_version == 0
    assert header.incremental_vacuum_mode == 0
    assert header.application_id == 0
    assert header.reserved_bytes_per_page == 0
    assert header.version_valid_for == 25
    assert header.sqlite_version_number == 3007006
