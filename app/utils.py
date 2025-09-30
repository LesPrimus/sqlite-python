import re
import struct
from dataclasses import dataclass, field

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Where
from sqlparse.tokens import Keyword


@dataclass
class ParsedCommand:
    table_name: str = field(default="")
    columns: list[str] = field(default_factory=list)
    function: str | None = None
    where: str | None = None


def extract_columns(sql_statement: str) -> dict[str, str]:
    # Pattern to match column definitions inside CREATE TABLE
    pattern = r"CREATE\s+TABLE\s+\w+\s*\(\s*([^)]+)\)"
    match = re.search(pattern, sql_statement)
    columns = {}
    if match:
        columns_str = match.group(1)
        for col in columns_str.split(","):
            # Extract name and type from each column definition
            col_match = re.match(r"\s*(\w+)\s+(\w+)\s*", col.strip())
            if col_match:
                name, type_ = col_match.groups()
                columns[name] = type_
    return columns


def parse_command(command: str) -> ParsedCommand:
    parsed = sqlparse.parse(command)[0]
    parsed_command = ParsedCommand()
    from_seen = False
    for token in parsed.tokens:
        if token.ttype is Keyword and token.value.upper() == "FROM":
            from_seen = True
        match token:
            case Where():
                parsed_command.where = (
                    token.value.replace("WHERE", "").replace("where", "").strip()
                )
            case Function():
                parsed_command.function = token.get_name()
            case IdentifierList():
                parsed_command.columns = [
                    sub_token.value
                    for sub_token in token.tokens
                    if not sub_token.is_whitespace and sub_token.value.strip() != ","
                ]
            case Identifier():
                if from_seen:
                    parsed_command.table_name = token.value
                    from_seen = False
                else:
                    parsed_command.columns = [token.value]
    return parsed_command


def get_serial_type_code(n: int) -> int | str:
    match n:
        case 0:
            return "NULL"
        case 1:
            return 1
        case 2:
            return 2
        case 3:
            return 3
        case 4:
            return 4
        case 5:
            return 6
        case 6:
            return 8
        case 7:
            return 8
        case 8:
            return 0
        case 9:
            return 0
        case 10 | 11:
            return "VARIABLE"
        case _ if n >= 12 and n % 2 == 0:
            return (n - 12) // 2
        case _ if n >= 13 and n % 2 != 0:
            return (n - 13) // 2
        case _:
            raise ValueError(f"Invalid serial type code: {n}")

def get_varint(buffer):
    result = 0
    while True:
        value = int.from_bytes(buffer.read(1), "big")
        result = (result << 7) | (value & 0x7F)
        if (value & 0x80) == 0:
            break
    return result

def get_offsets(buffer, page_size, cell_count):
    unpacked_offsets = struct.unpack(
        f">{cell_count}H", buffer.read(cell_count * 2)
    )
    return list(sorted((page_size, *unpacked_offsets)))
