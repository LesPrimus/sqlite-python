import re


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
