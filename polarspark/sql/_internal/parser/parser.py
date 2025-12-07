from typing import Generator
from sqlglot import parse, Expression


def parse_sql(sql: str) -> Generator[Expression, None, None]:
    statements = parse(sql, read="spark")

    for s in statements:
        yield s
