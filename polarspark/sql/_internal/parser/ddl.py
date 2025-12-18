from typing import TYPE_CHECKING
from shutil import rmtree
import sqlglot.expressions as expr

from polarspark.sql._internal.parser.models import SourceRelation, InsertInto, DropTable
from polarspark.sql.types import _parse_datatype_string
from polarspark.sql._internal.parser.parser import parse_sql
from polarspark.sql._internal.parser import ast

if TYPE_CHECKING:
    from polarspark.sql import SparkSession

AST_TYPE_MAP = {"TEXT": "STRING"}


def execute_create_table(spark: "SparkSession", ctx: SourceRelation):
    table_name = ctx.name
    schema = ["{} {}".format(col, AST_TYPE_MAP.get(ty, ty)) for col, ty in ctx.columns]
    schema = ", ".join(schema)
    spark.catalog.createTable(
        tableName=table_name,
        path=ctx.location,
        source=ctx.format,
        schema=_parse_datatype_string(schema),
    )


def execute_insert_into(spark: "SparkSession", ctx: InsertInto):
    tbl = spark.catalog._cat.get_table(ctx.table)  # noqa

    # Filter only requested columns
    cols = tbl.columns
    if ctx.columns:
        cols = [c for c in cols if c[0] in ctx.columns]

    schema = ["{}: {}".format(col, AST_TYPE_MAP.get(ty, ty)) for col, ty in cols]

    df = spark.createDataFrame(ctx.values or [], schema=", ".join(schema))
    df.write.mode("append").format(tbl.format).save(tbl.location)


def execute_drop_table(spark: "SparkSession", ctx: DropTable):
    ts = spark.catalog._cat.get_ts(ctx.table)  # noqa
    if ctx.table in ts.tables:
        tbl = ts.tables.pop(ctx.table)
        ts.pl_ctx.unregister(ctx.table)
        if tbl.location:
            rmtree(tbl.location)


def execute_sql(spark: "SparkSession", sql: str):
    for x in parse_sql(sql):
        if isinstance(x, expr.Create):
            ct = ast.create_table(x)
            execute_create_table(spark, ct)
            yield ct
        if isinstance(x, expr.Insert):
            ins = ast.insert_table(x)
            execute_insert_into(spark, ins)
            yield ins
        if isinstance(x, expr.Select):
            yield ast.select_tables(x)
        if isinstance(x, expr.Drop):
            drp = ast.drop_table(x)
            execute_drop_table(spark, drp)
            yield drp
