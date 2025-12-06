from typing import  TYPE_CHECKING
from shutil import rmtree
import sqlglot.expressions as expr

from polarspark.sql._internal.parser.models import CreateTable, InsertInto, DropTable
from polarspark.sql.types import _parse_datatype_string
from polarspark.sql._internal.parser.parser import parse_sql
from polarspark.sql._internal.parser import ast

if TYPE_CHECKING:
    from polarspark.sql import SparkSession

AST_TYPE_MAP = {
    "TEXT": "STRING"
}


def execute_create_table(spark: "SparkSession", ctx: CreateTable):
    table_name = ctx.name
    schema = ["{} {}".format(col, AST_TYPE_MAP.get(ty, ty))
              for col, ty
              in ctx.columns]
    schema = ", ".join(schema)
    print(f"Schema is: {schema} {_parse_datatype_string(schema)}")
    spark.catalog.createTable(
        tableName=table_name,
        path=ctx.location,
        source=ctx.format,
        schema=_parse_datatype_string(schema),
    )


def execute_insert_into(spark: "SparkSession", ctx: InsertInto):
    tbl = spark.catalog._cat.get_ts(ctx.table).tables[ctx.table]
    print(f"Table {ctx.table} is: {tbl}")
    print(f"Ctx {ctx}")
    schema = ["{} {}".format(col, AST_TYPE_MAP.get(ty, ty))
              for col, ty
              in tbl.columns if col in ctx.columns]

    print(ctx)
    df = spark.createDataFrame(ctx.values or [], schema=schema)
    df.show()
    df.write.mode("append").format(tbl.format).save(tbl.location)


def execute_drop_table(spark: "SparkSession", ctx: DropTable):
    ts = spark.catalog._cat.get_ts(ctx.table)
    if ctx.table in ts.tables:
        tbl = ts.tables.pop(ctx.table)
        ts.pl_ctx.unregister(ctx.table)
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
            yield ast.select_table(x)
        if isinstance(x, expr.Drop):
            drp = ast.drop_table(x)
            execute_drop_table(spark, drp)
            yield drp