import re
import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList

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

def parse_command(command):
    exclude = {" ", ","}
    parsed = sqlparse.parse(command)[0]

    values = []

    for token in parsed.tokens:
        match token:
            case Function():
                values.append(token.get_name())
            case IdentifierList():
                for sub_token in token.tokens:
                    if (value := sub_token.value) and value not in exclude:
                        values.append(value)
            case Identifier():
                if (value := token.value) and value not in exclude:
                    values.append(token.value)
    *columns, table_name = values
    return columns, table_name