from dataclasses import dataclass
from typing import Optional


@dataclass
class TableName:
    table: str
    database: Optional[str] = None
    catalog: Optional[str] = None


def parse_table_name(table_name: str) -> TableName:
    parts = table_name.split(".")

    if len(parts) == 3:
        return TableName(catalog=parts[0], database=parts[1], table=parts[2])
    elif len(parts) == 2:
        return TableName(database=parts[0], table=parts[1])
    elif len(parts) == 1:
        return TableName(table=parts[0])

    raise ValueError(f"Invalid table name: {table_name}")
