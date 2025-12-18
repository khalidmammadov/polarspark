![CI](https://github.com/khalidmammadov/polarspark/actions/workflows/ci.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/v/polarspark)
![Python](https://img.shields.io/pypi/pyversions/polarspark)
![License](https://img.shields.io/github/license/khalidmammadov/polarspark)
```
 ____       _              ____                   _    
|  _ \ ___ | | __ _ _ __  / ___| _ __   __ _ _ __| | __
| |_) / _ \| |/ _` | '__| \___ \| '_ \ / _` | '__| |/ /
|  __/ (_) | | (_| | |     ___) | |_) | (_| | |  |   < 
|_|   \___/|_|\__,_|_|    |____/| .__/ \__,_|_|  |_|\_\
                                |_|                    
```
# 🚀 Apache Spark on Polars

Polar Spark brings the PySpark API to Polars, optimized for single-machine workloads.

It is designed as a drop-in replacement for PySpark in scenarios where a full Spark cluster is not needed. A common use case is running fast, lightweight unit tests in CI/CD pipelines 🧪.

Instead of relying on the JVM-based Spark engine, Polar Spark runs on Polars’ Lazy API, powered by a high-performance Rust execution engine 🦀. This avoids the overhead of the JVM, which can be slow and heavy for small or local workloads.

By leveraging Polars, Polar Spark automatically benefits from:
* 🚀 Advanced query optimization
* 🧵 Efficient multithreading
* 🖥️ Excellent performance on modern CPUs

🎯 Goal: Make Polar Spark a seamless PySpark replacement whenever workloads fit on a single machine or within local resource limits.

## Installation
```shell
pip install polarspark
```

## Examples:
### Spark session
```python
try:            
    from polarspark.sql.session import SparkSession
except Exception:
    from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local").appName("myapp").getOrCreate()

print(spark)
print(type(spark))

>>> <polarspark.sql.session.SparkSession object at 0x1043bdd90>
>>> <class 'polarspark.sql.session.SparkSession'>
```

### DataFrame API
```python
try:
    from polarspark.sql import Row
    from polarspark.sql.types import *
except Exception:
    from pyspark.sql import Row
    from pyspark.sql.types import *    
from pprint import pprint
```
```python

d = [{'name': 'Alice', 'age': 1}, 
     {'name': 'Tome', 'age': 100}, 
     {'name': 'Sim', 'age': 99}]
df = spark.createDataFrame(d)
rows = df.collect()
```

### SQL
```python
spark.sql("CREATE TABLE input_table (value string) USING parquet")
spark.sql("INSERT INTO input_table VALUES (1), (2), (3)")

spark.sql("""
    SELECT * 
    FROM input_table i 
        JOIN my_table m 
    ON i.value = m.age
""").show()
```

### API
```python
pprint(rows)
>>> [Row(age=1, name='Alice'),
>>>  Row(age=100, name='Tome'),
>>>  Row(age=99, name='Sim')]
```

```python
df.printSchema()
>>> root
>>>  |-- age: long (nullable = true)
>>>  |-- name: string (nullable = true)
```

```python
# With schema
schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)])
df_no_rows = spark.createDataFrame([], schema=schema)

print(df_no_rows.isEmpty())
>>> True
```

```python
# or using Spark DDL
df = spark.createDataFrame([("Alice", 3), ("Ben", 5)], schema="name STRING, age INT")
print(df.isEmpty())
>>> False
```

### Read / write Parquet, Delta, CSV etc.
```python
base_path = "/var/tmp"

df1 = spark.read.format("json").load([f"{base_path}/data.json",
                                     f"{base_path}/data.json"
                                     ])
df2 = spark.read.json([f"{base_path}/data.json",
                      f"{base_path}/data.json"])


df1.write.format("csv").save(f"{base_path}/data_json_to_csv.csv", mode="overwrite")

df1 = spark.read.format("csv").load([f"{base_path}/data_json_to_csv.csv",
                                       f"{base_path}/data_json_to_csv.csv"])

df1 = spark.read.format("parquet").load([f"{base_path}/data_json_to_parquet.parquet",
                                       f"{base_path}/data_json_to_parquet.parquet"])
df2 = spark.read.parquet(f"{base_path}/data_json_to_parquet.parquet",
                               f"{base_path}/data_json_to_parquet.parquet")
```

### Streaming (Stateless)
```python
df = self.spark.readStream.format("rate").load()
q = df.writeStream.toTable("output_table", format="parquet", checkpointLocation=tmpdir)
q.stop()
result = self.spark.sql("SELECT value FROM output_table").collect()    
```

### Streaming (foreachBatch)
```python
def collectBatch(batch_df, batch_id):
    batch_df.write.format("parquet").mode("overwrite").saveAsTable("test_table1")

df = self.spark.readStream.format("text").load("polarspark/test_support/sql/streaming")
q = df.writeStream.foreachBatch(collectBatch).start()
q.processAllAvailable()
collected = self.spark.sql("select * from test_table1").collect()
```

### In Memory Catalog
```python
df.write.saveAsTable("my_table")
spark.sql("select * from my_table").show()
```

## Some more:

### Filter
```python
pprint(df.offset(1).first())
>>>  Row(age=100, name='Tome')
```

```python
df.show()

shape: (3, 2)
┌─────┬──────────┐
│ age ┆ name     │
│ --- ┆ ---      │
│ i64 ┆ str      │
╞═════╪══════════╡
│ 1   ┆ Alice    │
│ 100 ┆ Tome     │
│ 99  ┆ Sim      │
└─────┴──────────┘
```

```python
df.explain()
                 0
   ┌─────────────────────────
   │
   │  ╭─────────────────────╮
   │  │ DF ["age", "name"]  │
 0 │  │ PROJECT */2 COLUMNS │
   │  ╰─────────────────────╯
```

```python
print(repr(df))
>>>  DataFrame[age: bigint, name: string]
print(df.count())
>>>  3
```

```python
def func(row):
    print("Row -> {}".format(row))

df.foreach(func)

df = spark.createDataFrame(
    [(14, "Tom"), (23, "Alice"), (16, "Bob"), (16, "Bob")], ["age", "name"]
)

def func(itr):
    for person in itr:
        print(person)
        print("Person -> {}".format(person.name))
df.foreachPartition(func)

df.show()
df.distinct().show()
```


NOTE: Some of the features are not directly mapped but relies on Polars. 
e.g. df.show() or df.explain() will print polars relevant method output 

