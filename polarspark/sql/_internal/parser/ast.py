from typing import Optional

from sqlglot.expressions import (
    Create,
    Table,
    Identifier,
    Schema,
    FileFormatProperty,
    LocationProperty,
    PartitionedByProperty,
    ColumnDef,
    Insert,
    Expression,
    Select,
    Drop,
    Literal,
)

from polarspark.sql._internal.parser.models import SourceRelation, InsertInto, SelectFrom, DropTable


def get_schema(expr: Expression) -> Optional[Expression]:
    return expr.find(Schema)


def get_table(expr: Expression) -> Optional[Expression]:
    return expr.find(Table)


def get_tables(expr: Expression) -> Optional[list[Expression]]:
    return list(expr.find_all(Table))


def get_name(expr: Optional[Expression] = None, sbj: str = "this") -> Optional[str]:
    """
    Get name from Identifier in any Expression
    """
    if expr:
        res = expr.args.get(sbj)
        if res:
            return str(res.this)
    return None


def get_columns(expr: Expression) -> list[tuple[str]]:
    columns = []
    for col in expr.find_all(ColumnDef):
        col_name = col.name
        col_type = col.args["kind"].sql()  # e.g. INT, DOUBLE
        columns.append((col_name, col_type))
    return columns


def get_format(expr: Expression) -> Optional[str]:
    if formats := expr.find(FileFormatProperty):
        return str(formats.this)
    return None


def get_location(expr: Expression) -> Optional[str]:
    if locs := expr.find(LocationProperty):
        return locs.this.this
    return None


def get_partitioned_by(expr: Expression) -> Optional[list[str]]:
    if part := expr.find(PartitionedByProperty):
        return [str(i) for i in part.find_all(Identifier)]
    return None


def create_table(expr: Create) -> SourceRelation:
    tbl = get_table(expr)
    table_name = get_name(tbl)
    db = get_name(tbl, sbj="db")

    columns = get_columns(expr)
    partitioned_by = get_partitioned_by(expr)
    location = get_location(expr)
    file_format = get_format(expr)
    return SourceRelation(
        name=table_name,
        db=db,
        columns=columns,
        format=file_format,
        partitioned_by=partitioned_by,
        location=location,
    )


def select_tables(expr: Select) -> SelectFrom:
    tables = get_tables(expr)
    names = [get_name(tbl) for tbl in tables if tbl]
    return SelectFrom(tables=names)


def drop_table(expr: Drop) -> DropTable:
    tbl = get_table(expr)
    table_name = get_name(tbl)
    db = get_name(tbl, sbj="db")
    return DropTable(table=table_name, db=db)


def get_insert_cols(expr: Expression) -> Optional[list[str]]:
    if schema := get_schema(expr):
        if idents := schema.expressions:
            return [i.this for i in idents]
    return None


def convert(l: Literal):
    if l.is_int:
        return int(l.this)
    if l.is_number:
        return float(l.this)
    return l.this


def get_insert_values(expr: Expression) -> Optional[list]:
    if values := expr.args.get("expression"):
        if tuples := values.expressions:
            return [[convert(l) for l in t.expressions] for t in tuples]
    return None


def insert_table(expr: Insert) -> InsertInto:
    tbl = get_table(expr)
    return InsertInto(
        table=get_name(tbl),
        db=get_name(tbl, sbj="db"),
        columns=get_insert_cols(expr) or [],
        values=get_insert_values(expr),
    )
