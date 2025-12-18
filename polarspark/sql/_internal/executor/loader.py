from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from polarspark.sql import SparkSession
    from polarspark.sql.dataframe import DataFrame
    from polarspark.sql.types import StructType


def load_df_from_location(
    spark: "SparkSession",
    path: str,
    source: str,
    schema: Optional["StructType"] = None,
    **options: str,
) -> "DataFrame":
    reader = spark.read
    reader = reader.format(source)
    if schema:
        reader = reader.schema(schema)

    return reader.options(**options).load(path)
