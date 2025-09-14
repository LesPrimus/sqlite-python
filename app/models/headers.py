from dataclasses import dataclass, fields

__all__ = ["DbHeader", "LeafPageHeader"]


class ParseHeaderMixin:
    @classmethod
    def _parse_field_type(cls, buffer, _type, size):
        if _type is int:
            return int.from_bytes(buffer.read(size), "big")
        elif _type is str:
            return buffer.read(size).decode("utf-8")
        else:
            raise ValueError(f"Unsupported field type: {_type}")

    @classmethod
    def from_bytes(cls, buffer):
        results = []
        for field in fields(cls):  # noqa
            field_size = cls._FIELD_SIZE[field.name]  # noqa
            results.append(cls._parse_field_type(buffer, field.type, field_size))
        return cls(*results)  # noqa


@dataclass
class DbHeader(ParseHeaderMixin):
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

    _FIELD_SIZE = {
        "magic_header_str": 16,
        "page_size": 2,
        "file_format_write_version_number": 1,
        "file_format_read_version_number": 1,
        "reserved_bytes_per_pages": 1,
        "max_embedded_payload_fraction": 1,
        "min_embedded_payload_fraction": 1,
        "leaf_payload_fraction": 1,
        "file_change_counter": 4,
        "size_in_pages": 4,
        "freelist_trunk_page_number": 4,
        "freelist_pages_number": 4,
        "schema_cookie": 4,
        "schema_format_number": 4,
        "default_page_cache_size": 4,
        "largest_root_page_number": 4,
        "db_text_encoding": 4,
        "user_version": 4,
        "incremental_vacuum_mode": 4,
        "application_id": 4,
        "reserved_bytes_per_page": 20,
        "version_valid_for": 4,
        "sqlite_version_number": 4,
    }


@dataclass
class LeafPageHeader(ParseHeaderMixin):
    page_type: int
    first_free_block: int
    cell_count: int
    cell_content_area: int
    fragment_free_bytes: int

    _FIELD_SIZE = {
        "page_type": 1,
        "first_free_block": 2,
        "cell_count": 2,
        "cell_content_area": 2,
        "fragment_free_bytes": 1,
    }
