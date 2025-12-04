from typing import  TYPE_CHECKING
import sqlglot.expressions as expr

import polarspark.sql._internal.parser.models
from polarspark.sql.types import _parse_datatype_string
from polarspark.sql._internal.parser.parser import parse_sql
from polarspark.sql._internal.parser import ast

if TYPE_CHECKING:
    from polarspark.sql import SparkSession

DEFAULT_SPARK_PATH = "spark-warehouse"

AST_TYPE_MAP = {
    "TEXT": "STRING"
}


def execute_create_table(spark: SparkSession, ctx: polarspark.sql._internal.parser.models.CreateTable):
    table_name = ctx.name
    schema = ["{} {}".format(col, AST_TYPE_MAP.get(ty, ty))
              for col, ty
              in ctx.columns]
    schema = ", ".join(schema)
    spark.catalog.createTable(
        tableName=table_name,
        path=ctx.location,
        source=ctx.format,
        schema = _parse_datatype_string(schema),
    )


def execute_insert_into(spark: SparkSession, ctx: polarspark.sql._internal.parser.models.InsertInto):
    schema = ["{} {}".format(col, AST_TYPE_MAP.get(ty, ty))
              for col, ty
              in ctx.columns]
    catalog_tbl = spark.catalog.getTable(ctx.table)
    df = spark.createDataFrame([], schema=schema)
    df.write.mode("append").format(ctx.).parquet(ctx.location)

def execute_sql(spark: SparkSession, sql: str) -> bool:
    for x in parse_sql(sql):
        if isinstance(x, expr.Create):
            ct = ast.create_table(x)
            execute_create_table(spark, ct)
            return True
        if isinstance(x, expr.Insert):
            ins = ast.insert_table(x)
            execute_insert_into(spark, ins)
            return True
    return False