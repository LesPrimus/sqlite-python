import re
import struct
from dataclasses import dataclass, field

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Where, Parenthesis
from sqlparse.tokens import Keyword


@dataclass
class ParsedCommand:
    table_name: str = field(default="")
    columns: list[str] = field(default_factory=list)
    function: str | None = None
    where: str | None = None


def extract_columns(sql_statement: str) -> list[str]:
    parsed = sqlparse.parse(sql_statement)[0]
    columns = []

    for token in parsed.tokens:
        if isinstance(token, Parenthesis):
            for sub_token in token.tokens:
                if isinstance(sub_token, Identifier):
                    columns.append(sub_token.value)
                if isinstance(sub_token, IdentifierList):
                    for sub_sub_token in sub_token.get_identifiers():
                        if isinstance(sub_sub_token, Identifier):
                            columns.append(sub_sub_token.value)

    columns = [column.lower() for column in columns if
               column not in {"autoincrement", "primary key", "not null", "text", "integer", "year"}]

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

def get_offsets(buffer, page_size, cell_count, page_nr=1):
    unpacked_offsets = struct.unpack(f">{cell_count}H", buffer.read(cell_count * 2))
    print(unpacked_offsets)
    offsets = (page_size, *unpacked_offsets)
    offsets = [offset + page_size * (page_nr - 1) for offset in offsets]
    return list(sorted(offsets))