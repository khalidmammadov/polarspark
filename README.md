```
 ____       _              ____                   _    
|  _ \ ___ | | __ _ _ __  / ___| _ __   __ _ _ __| | __
| |_) / _ \| |/ _` | '__| \___ \| '_ \ / _` | '__| |/ /
|  __/ (_) | | (_| | |     ___) | |_) | (_| | |  |   < 
|_|   \___/|_|\__,_|_|    |____/| .__/ \__,_|_|  |_|\_\
                                |_|                    
```
# Apache PySpark on Polars.

**Polar Spark** is PySpark on [Polars](https://github.com/pola-rs/polars) for single machine workloads.

It runs on Polars' Lazy API which is backed by powerful Rust engine
whereas classic PySpark depends on JVM/Java based engine
which is slow for these types of workloads.

It benefits all the performance improvements and optimizations **polars** provides 
to run on a multithreaded environment with modern CPUs.

So, the aim is to make **Polar Spark** drop in replacement for PySpark
where PySpark is used on single machine or where data can fit into
resources of a single machine.

Examples:
### Create spark session
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

### Create DataFrame
```python
try:
    from polarspark.sql import Row
    from polarspark.sql.types import *
except Exception:
    from pyspark.sql import Row
    from pyspark.sql.types import *
    
from pprint import pprint

d = [{'name': 'Alice', 'age': 1}, 
     {'name': 'Tome', 'age': 100}, 
     {'name': 'Sim', 'age': 199}]
df = spark.createDataFrame(d)
rows = df.collect()

pprint(rows)
>>> [Row(age=1, name='Alice'),
>>>  Row(age=100, name='Tome'),
>>>  Row(age=199, name='Sim')]

pprint(df.offset(1).first())
>>>  Row(age=100, name='Tome')

schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)])
df_no_rows = spark.createDataFrame([], schema=schema)
print(df_no_rows.isEmpty())

>>> True

```
### Read and write Parquet, Delta, CSV etc.
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



Some more:
```python
df.show()

pprint(df.tail(2))

shape: (3, 2)
┌─────┬──────────┐
│ age ┆ name     │
│ --- ┆ ---      │
│ i64 ┆ str      │
╞═════╪══════════╡
│ 1   ┆ Alice    │
│ 100 ┆ Tome     │
│ 199 ┆ Sim      │
└─────┴──────────┘

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

