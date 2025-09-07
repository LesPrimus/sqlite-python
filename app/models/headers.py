from dataclasses import dataclass

__all__ = ["DbHeader", "LeafPageHeader"]


@dataclass
class DbHeader:
    magic_header_str: str
    page_size: int
    file_format_write_version_number: int
    file_format_read_version_number: int
    reserved_bytes_per_pages: int
    max_embedded_payload_fraction: int
    min_embedded_payload_fraction: int
    leaf_payload_fraction: int
    file_change_counter: int
    size_in_pages: int
    freelist_trunk_page_number: int
    freelist_pages_number: int
    schema_cookie: int
    schema_format_number: int
    default_page_cache_size: int
    largest_root_page_number: int
    db_text_encoding: int
    user_version: int
    incremental_vacuum_mode: int
    application_id: int
    reserved_bytes_per_page: int
    version_valid_for: int
    sqlite_version_number: int

    @classmethod
    def from_bytes(cls, buffer):
        magic_header_str = buffer.read(16).decode("utf-8")
        page_size = int.from_bytes(buffer.read(2), "big")
        file_format_write_version_number = int.from_bytes(buffer.read(1), "big")
        file_format_read_version_number = int.from_bytes(buffer.read(1), "big")
        reserved_bytes_per_pages = int.from_bytes(buffer.read(1), "big")
        max_embedded_payload_fraction = int.from_bytes(buffer.read(1), "big")
        min_embedded_payload_fraction = int.from_bytes(buffer.read(1), "big")
        leaf_payload_fraction = int.from_bytes(buffer.read(1), "big")
        file_change_counter = int.from_bytes(buffer.read(4), "big")
        size_in_pages = int.from_bytes(buffer.read(4), "big")
        freelist_trunk_page_number = int.from_bytes(buffer.read(4), "big")
        freelist_pages_number = int.from_bytes(buffer.read(4), "big")
        schema_cookie = int.from_bytes(buffer.read(4), "big")
        schema_format_number = int.from_bytes(buffer.read(4), "big")
        default_page_cache_size = int.from_bytes(buffer.read(4), "big")
        largest_root_page_number = int.from_bytes(buffer.read(4), "big")
        db_text_encoding = int.from_bytes(buffer.read(4), "big")
        user_version = int.from_bytes(buffer.read(4), "big")
        incremental_vacuum_mode = int.from_bytes(buffer.read(4), "big")
        application_id = int.from_bytes(buffer.read(4), "big")
        reserved_bytes_per_page = int.from_bytes(buffer.read(20), "big")
        version_valid_for = int.from_bytes(buffer.read(4), "big")
        sqlite_version_number = int.from_bytes(buffer.read(4), "big")

        return cls(
            magic_header_str,
            page_size,
            file_format_write_version_number,
            file_format_read_version_number,
            reserved_bytes_per_pages,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            size_in_pages,
            freelist_trunk_page_number,
            freelist_pages_number,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            largest_root_page_number,
            db_text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            reserved_bytes_per_page,
            version_valid_for,
            sqlite_version_number,
        )


@dataclass
class LeafPageHeader:
    page_type: int
    first_free_block: int
    cell_count: int
    cell_content_area: int
    fragment_free_bytes: int

    @classmethod
    def from_bytes(cls, buffer):
        page_type = int.from_bytes(buffer.read(1), "big")
        first_free_block = int.from_bytes(buffer.read(2), "big")
        cell_count = int.from_bytes(buffer.read(2), "big")
        cell_content_area = int.from_bytes(buffer.read(2), "big")
        fragment_free_bytes = int.from_bytes(buffer.read(1), "big")
        return cls(
            page_type,
            first_free_block,
            cell_count,
            cell_content_area,
            fragment_free_bytes,
        )
