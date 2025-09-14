from argparse import ArgumentParser
from pathlib import Path

from app.parser import SqliteParser


def get_args():
    parser = ArgumentParser()
    parser.add_argument("database_file_path", type=Path)
    parser.add_argument("command", type=str)
    return parser.parse_args()


def main(*, database_file_path: Path, command: str):
    with SqliteParser(database_file_path) as parser:
        parser.handle_command(command)


if __name__ == "__main__":
    namespace = get_args()
    main(database_file_path=namespace.database_file_path, command=namespace.command)
