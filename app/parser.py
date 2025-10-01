from os import PathLike

__all__ = ["SqliteParser"]

from app.models.tables import RootPage


class SqliteParser:
    def __init__(self, db_path: PathLike):
        self.file_object = db_path.open("rb")  # noqa
        self.root_page = None

    def __enter__(self):
        self.file_object.__enter__()
        self.root_page = RootPage.from_bytes(self.file_object)
        return self

    def __exit__(self, *args):
        return self.file_object.__exit__(*args)

    def handle_db_info(self, verbose=False):
        db_header = self.root_page.db_header
        ret = [
            f"database page size: {db_header.page_size}",
            f"number of tables: {len(self.root_page.cells)}",
        ]
        if verbose:
            print("\n".join(ret))
        return ret

    def handle_command(self, command: str):
        match command:
            case ".dbinfo":
                return self.handle_db_info(verbose=True)
            case _:
                raise ValueError(f"Unknown command: {command}")
