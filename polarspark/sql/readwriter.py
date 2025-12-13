#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import pathlib
import sys
from typing import cast, overload, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING, Union
from functools import partial, reduce
from pathlib import Path
import shutil
import uuid

from polarspark import RDD, since
from polarspark.sql._internal.catalog.utils import parse_table_name

# from polarspark.sql.column import _to_seq, _to_java_column, Column
from polarspark.sql.column import Column
from polarspark.sql.pandas.conversion import schema_from_polars
from polarspark.sql.types import StructType, _parse_datatype_string
from polarspark.sql import utils
from polarspark.sql.utils import to_str
from polarspark.errors import PySparkTypeError, PySparkValueError, PySparkRuntimeError

import polars as pl

if TYPE_CHECKING:
    from polarspark.sql._typing import OptionalPrimitiveType, ColumnOrName
    from polarspark.sql.session import SparkSession
    from polarspark.sql.dataframe import DataFrame

    # from polarspark.sql.streaming import StreamingQuery

__all__ = ["DataFrameReader", "DataFrameWriter", "DataFrameWriterV2"]

PathOrPaths = Union[str, List[str]]
TupleOrListOfString = Union[List[str], Tuple[str, ...]]


class OptionUtils:
    def _set_opts(
        self,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)  # type: ignore[attr-defined]
        for k, v in options.items():
            if v is not None:
                self.option(k, v)  # type: ignore[attr-defined]


class DataFrameReader(OptionUtils):
    """
    Interface used to load a :class:`DataFrame` from external storage systems
    (e.g. file systems, key-value stores, etc). Use :attr:`SparkSession.read`
    to access this.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, spark: "SparkSession"):
        self._options = {}
        self._format = None
        self._spark = spark
        self._schema: Optional[StructType] = None

    def format(self, source: str) -> "DataFrameReader":
        """Specifies the input data source format.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.read.format('json')
        <...readwriter.DataFrameReader object ...>

        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        # self._jreader = self._jreader.format(source)
        self._format = source
        return self

    def schema(self, schema: Union[StructType, str]) -> "DataFrameReader":
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        schema : :class:`polarspark.sql.types.StructType` or str
            a :class:`polarspark.sql.types.StructType` object or a DDL-formatted string
            (For example ``col0 INT, col1 DOUBLE``).

        Examples
        --------
        >>> spark.read.schema("col0 INT, col1 DOUBLE")
        <...readwriter.DataFrameReader object ...>

        Specify the schema with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     spark.read.schema("col0 INT, col1 DOUBLE").format("csv").load(d).printSchema()
        root
         |-- col0: integer (nullable = true)
         |-- col1: double (nullable = true)
        """

        if isinstance(schema, StructType):
            self._schema = schema
        elif isinstance(schema, str):
            self._schema = _parse_datatype_string(schema)
        else:
            raise PySparkTypeError(
                error_class="NOT_STR_OR_STRUCT",
                message_parameters={
                    "arg_name": "schema",
                    "arg_type": type(schema).__name__,
                },
            )
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds an input option for the underlying data source.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key : str
            The key for the option to set.
        value
            The value for the option to set.

        Examples
        --------
        >>> spark.read.option("key", "value")
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.schema(df.schema).option(
        ...         "nullValue", "Hyukjin Kwon").format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._options[key] = to_str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameReader":
        """
        Adds input options for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and prmitive-type values.

        Examples
        --------
        >>> spark.read.options(key="value")
        <...readwriter.DataFrameReader object ...>

        Specify options in a dictionary.

        >>> spark.read.options(**{"k1": "v1", "k2": "v2"})
        <...readwriter.DataFrameReader object ...>

        Specify the option 'nullValue' and 'header' with reading a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with a header.
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     spark.read.options(
        ...         nullValue="Hyukjin Kwon",
        ...         header=True
        ...     ).format('csv').load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._options.update(options)
        return self

    def load(
        self,
        path: Optional[PathOrPaths] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        """Loads data from a data source and returns it as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list, optional
            optional string or a list of string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source. Default to 'parquet'.
        schema : :class:`polarspark.sql.types.StructType` or str, optional
            optional :class:`polarspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Examples
        --------
        Load a CSV file with format, schema and options specified.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with a header
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.option("header", True).mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.read.load(
        ...         d, schema=df.schema, format="csv", nullValue="Hyukjin Kwon", header=True)
        ...     df.printSchema()
        ...     df.show()
        root
         |-- age: long (nullable = true)
         |-- name: string (nullable = true)
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        if format:
            self.format(format)
        if schema:
            self.schema(schema)
        self.options(**options)

        def get_reader(source: str):
            if source == "text":
                _col = self._schema.names[0] if self._schema else "value"
                return partial(
                    self._read_ldf,
                    reader=partial(
                        pl.scan_csv, infer_schema=False, has_header=False, schema={_col: pl.String}
                    ),
                )
            elif source == "csv":
                return partial(self._read_ldf, reader=pl.scan_csv)
            elif source == "json":
                return partial(self._read_paths_with_concat, reader=pl.read_json)
            elif source == "parquet":
                return partial(self._read_ldf, reader=pl.scan_parquet)
            elif source == "delta":
                return partial(self._read_ldf, reader=pl.scan_delta)
            elif source == "avro":
                return partial(self._read_paths_with_concat, reader=pl.read_avro)
            elif source == "excel":
                return partial(self._read_paths_with_concat, reader=pl.read_excel)
            else:
                raise NotImplementedError(f"Source type {source} not supported")

        reader = get_reader(self._format)

        _path = self._options.pop("path", None)  # Remove from options to avoid passing it down
        return reader(path or _path)

    def _read_ldf(self, path, reader) -> "DataFrame":
        from polarspark.sql.dataframe import DataFrame

        ldf = reader(path, **self._options)
        sample = ldf.first().collect()

        def df_generator():
            # This not dup. LazyFrame's sources are evaluated during scan initialisation
            # hence for paths we need to read when requested
            yield reader(path, **self._options)

        df = DataFrame(None, df_generator, self._spark)
        df._schema = schema_from_polars(sample)
        return df

    def _read_paths_with_concat(self, path, reader):
        from polarspark.sql.dataframe import DataFrame

        _paths = path
        if isinstance(path, str):
            _paths = [path]

        if type(_paths) != list:
            raise PySparkTypeError(
                error_class="NOT_STR_OR_LIST_OF_RDD",
                message_parameters={
                    "arg_name": "path",
                    "arg_type": type(_paths).__name__,
                },
            )

        # Check if path is a directory of files
        paths = []
        for p in _paths:
            _p = Path(p)
            if _p.is_dir():
                for i in _p.iterdir():
                    paths.append(i)
            else:
                paths.append(p)

        pdfs = []
        for p in paths:
            pdf = reader(p, **self._options)
            pdfs.append(pdf)
        pdf = reduce(lambda a, b: pl.concat([a, b]), pdfs)

        def df_generator():
            yield pdf.lazy()

        df = DataFrame(None, df_generator, self._spark)
        df._schema = schema_from_polars(pdf)
        return df

    def json(
        self,
        path: Union[str, List[str], RDD[str]],
        schema: Optional[Union[StructType, str]] = None,
        primitivesAsString: Optional[Union[bool, str]] = None,
        prefersDecimal: Optional[Union[bool, str]] = None,
        allowComments: Optional[Union[bool, str]] = None,
        allowUnquotedFieldNames: Optional[Union[bool, str]] = None,
        allowSingleQuotes: Optional[Union[bool, str]] = None,
        allowNumericLeadingZero: Optional[Union[bool, str]] = None,
        allowBackslashEscapingAnyCharacter: Optional[Union[bool, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        allowUnquotedControlChars: Optional[Union[bool, str]] = None,
        lineSep: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        encoding: Optional[str] = None,
        locale: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads JSON files and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string represents path to the JSON dataset, or a list of paths,
            or RDD of Strings storing JSON objects.
        schema : :class:`polarspark.sql.types.StructType` or str, optional
            an optional :class:`polarspark.sql.types.StructType` for the input schema or
            a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.json(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._set_opts(
            schema=schema,
            primitivesAsString=primitivesAsString,
            prefersDecimal=prefersDecimal,
            allowComments=allowComments,
            allowUnquotedFieldNames=allowUnquotedFieldNames,
            allowSingleQuotes=allowSingleQuotes,
            allowNumericLeadingZero=allowNumericLeadingZero,
            allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            multiLine=multiLine,
            allowUnquotedControlChars=allowUnquotedControlChars,
            lineSep=lineSep,
            samplingRatio=samplingRatio,
            dropFieldIfAllNull=dropFieldIfAllNull,
            encoding=encoding,
            locale=locale,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            allowNonNumericNumbers=allowNonNumericNumbers,
        )
        self.format("json")
        return self.load(path)

    def table(self, tableName: str) -> "DataFrame":
        """Returns the specified table as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        tableName : str
            string, name of the table.

        Examples
        --------
        >>> df = spark.range(10)
        >>> df.createOrReplaceTempView('tblA')
        >>> spark.read.table('tblA').show()
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        |  5|
        |  6|
        |  7|
        |  8|
        |  9|
        +---+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        return self._spark.sql(f"SELECT * FROM {tableName}")

    def parquet(self, *paths: str, **options: "OptionalPrimitiveType") -> "DataFrame":
        """
        Loads Parquet files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        paths : str
            One or more file paths to read the Parquet files from.

        Other Parameters
        ----------------
        **options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Returns
        -------
        :class:`DataFrame`
            A DataFrame containing the data from the Parquet files.

        Examples
        --------
        Create sample dataframes.

        >>> df = spark.createDataFrame(
        ...     [(10, "Alice"), (15, "Bob"), (20, "Tom")], schema=["age", "name"])
        >>> df2 = spark.createDataFrame([(70, "Alice"), (80, "Bob")], schema=["height", "name"])

        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file.
        ...     df.write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).orderBy("name").show()
        +---+-----+
        |age| name|
        +---+-----+
        | 10|Alice|
        | 15|  Bob|
        | 20|  Tom|
        +---+-----+

        Read a Parquet file with a specific column.

        >>> with tempfile.TemporaryDirectory() as d:
        ...     df.write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file with only the 'name' column.
        ...     spark.read.schema("name string").parquet(d).orderBy("name").show()
        +-----+
        | name|
        +-----+
        |Alice|
        |  Bob|
        |  Tom|
        +-----+

        Read multiple Parquet files and merge schema.

        >>> with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
        ...     df.write.mode("overwrite").format("parquet").save(d1)
        ...     df2.write.mode("overwrite").format("parquet").save(d2)
        ...
        ...     spark.read.option(
        ...         "mergeSchema", "true"
        ...     ).parquet(d1, d2).select(
        ...         "name", "age", "height"
        ...     ).orderBy("name", "age").show()
        +-----+----+------+
        | name| age|height|
        +-----+----+------+
        |Alice|NULL|    70|
        |Alice|  10|  NULL|
        |  Bob|NULL|    80|
        |  Bob|  15|  NULL|
        |  Tom|  20|  NULL|
        +-----+----+------+
        """
        mergeSchema = options.get("mergeSchema", None)
        pathGlobFilter = options.get("pathGlobFilter", None)
        modifiedBefore = options.get("modifiedBefore", None)
        modifiedAfter = options.get("modifiedAfter", None)
        recursiveFileLookup = options.get("recursiveFileLookup", None)
        datetimeRebaseMode = options.get("datetimeRebaseMode", None)
        int96RebaseMode = options.get("int96RebaseMode", None)
        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            datetimeRebaseMode=datetimeRebaseMode,
            int96RebaseMode=int96RebaseMode,
        )

        return self._read_ldf(paths, pl.scan_parquet)

    def text(
        self,
        paths: PathOrPaths,
        wholetext: bool = False,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads text files and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.
        The text files must be encoded as UTF-8.

        By default, each line in the text file is a new row in the resulting DataFrame.

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        paths : str or list
            string, or list of strings, for input path(s).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a text file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a text file
        ...     df = spark.createDataFrame([("a",), ("b",), ("c",)], schema=["alphabets"])
        ...     df.write.mode("overwrite").format("text").save(d)
        ...
        ...     # Read the text file as a DataFrame.
        ...     spark.read.schema(df.schema).text(d).sort("alphabets").show()
        +---------+
        |alphabets|
        +---------+
        |        a|
        |        b|
        |        c|
        +---------+
        """
        self._set_opts(
            wholetext=wholetext,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
        )

        if isinstance(paths, str):
            paths = [paths]
        assert self._spark._sc._jvm is not None
        return self._df(self._jreader.text(self._spark._sc._jvm.PythonUtils.toSeq(paths)))

    def csv(
        self,
        path: PathOrPaths,
        schema: Optional[Union[StructType, str]] = None,
        sep: Optional[str] = None,
        encoding: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        comment: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        nanValue: Optional[str] = None,
        positiveInf: Optional[str] = None,
        negativeInf: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        maxColumns: Optional[Union[int, str]] = None,
        maxCharsPerColumn: Optional[Union[int, str]] = None,
        maxMalformedLogPerPartition: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        r"""Loads a CSV file and returns the result as a  :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s),
            or RDD of Strings storing CSV rows.
        schema : :class:`polarspark.sql.types.StructType` or str, optional
            an optional :class:`polarspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.csv(d, schema=df.schema, nullValue="Hyukjin Kwon").show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self._set_opts(
            schema=schema,
            sep=sep,
            encoding=encoding,
            quote=quote,
            escape=escape,
            comment=comment,
            header=header,
            inferSchema=inferSchema,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            nullValue=nullValue,
            nanValue=nanValue,
            positiveInf=positiveInf,
            negativeInf=negativeInf,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            maxColumns=maxColumns,
            maxCharsPerColumn=maxCharsPerColumn,
            maxMalformedLogPerPartition=maxMalformedLogPerPartition,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            samplingRatio=samplingRatio,
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            modifiedBefore=modifiedBefore,
            modifiedAfter=modifiedAfter,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )
        self.format("csv")
        return self.load(path)

    def xml(
        self,
        path: Union[str, List[str], RDD[str]],
        rowTag: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        excludeAttribute: Optional[Union[bool, str]] = None,
        attributePrefix: Optional[str] = None,
        valueTag: Optional[str] = None,
        ignoreSurroundingSpaces: Optional[Union[bool, str]] = None,
        rowValidationXSDPath: Optional[str] = None,
        ignoreNamespace: Optional[Union[bool, str]] = None,
        wildcardColName: Optional[str] = None,
        encoding: Optional[str] = None,
        inferSchema: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        mode: Optional[str] = None,
        columnNameOfCorruptRecord: Optional[str] = None,
        multiLine: Optional[Union[bool, str]] = None,
        samplingRatio: Optional[Union[float, str]] = None,
        locale: Optional[str] = None,
    ) -> "DataFrame":
        r"""Loads a XML file and returns the result as a  :class:`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        path : str, list or :class:`RDD`
            string, or list of strings, for input path(s),
            or RDD of Strings storing XML rows.
        schema : :class:`polarspark.sql.types.StructType` or str, optional
            an optional :class:`polarspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a XML file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a XML file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").option("rowTag", "person").format("xml").save(d)
        ...
        ...     # Read the XML file as a DataFrame.
        ...     spark.read.option("rowTag", "person").xml(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        # self._set_opts(
        #     rowTag=rowTag,
        #     schema=schema,
        #     excludeAttribute=excludeAttribute,
        #     attributePrefix=attributePrefix,
        #     valueTag=valueTag,
        #     ignoreSurroundingSpaces=ignoreSurroundingSpaces,
        #     rowValidationXSDPath=rowValidationXSDPath,
        #     ignoreNamespace=ignoreNamespace,
        #     wildcardColName=wildcardColName,
        #     encoding=encoding,
        #     inferSchema=inferSchema,
        #     nullValue=nullValue,
        #     dateFormat=dateFormat,
        #     timestampFormat=timestampFormat,
        #     mode=mode,
        #     columnNameOfCorruptRecord=columnNameOfCorruptRecord,
        #     multiLine=multiLine,
        #     samplingRatio=samplingRatio,
        #     locale=locale,
        # )
        # if isinstance(path, str):
        #     path = [path]
        # if type(path) == list:
        #     assert self._spark._sc._jvm is not None
        #     return self._df(self._jreader.xml(self._spark._sc._jvm.PythonUtils.toSeq(path)))
        # elif isinstance(path, RDD):
        #
        #     def func(iterator: Iterable) -> Iterable:
        #         for x in iterator:
        #             if not isinstance(x, str):
        #                 x = str(x)
        #             if isinstance(x, str):
        #                 x = x.encode("utf-8")
        #             yield x
        #
        #     keyed = path.mapPartitions(func)
        #     keyed._bypass_serializer = True  # type: ignore[attr-defined]
        #     assert self._spark._jvm is not None
        #     jrdd = keyed._jrdd.map(self._spark._jvm.BytesToString())
        #     # There isn't any jvm api for creating a dataframe from rdd storing XML.
        #     # We can do it through creating a jvm dataset first and using the jvm api
        #     # for creating a dataframe from dataset storing XML.
        #     jdataset = self._spark._jsparkSession.createDataset(
        #         jrdd.rdd(), self._spark._jvm.Encoders.STRING()
        #     )
        #     return self._df(self._jreader.xml(jdataset))
        # else:
        #     raise PySparkTypeError(
        #         error_class="NOT_STR_OR_LIST_OF_RDD",
        #         message_parameters={
        #             "arg_name": "path",
        #             "arg_type": type(path).__name__,
        #         },
        #     )
        raise NotImplementedError()

    def orc(
        self,
        path: PathOrPaths,
        mergeSchema: Optional[bool] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        modifiedBefore: Optional[Union[bool, str]] = None,
        modifiedAfter: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """Loads ORC files, returning the result as a :class:`DataFrame`.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a ORC file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a ORC file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("orc").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.orc(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        # self._set_opts(
        #     mergeSchema=mergeSchema,
        #     pathGlobFilter=pathGlobFilter,
        #     modifiedBefore=modifiedBefore,
        #     modifiedAfter=modifiedAfter,
        #     recursiveFileLookup=recursiveFileLookup,
        # )
        # if isinstance(path, str):
        #     path = [path]
        # return self._df(self._jreader.orc(_to_seq(self._spark._sc, path)))
        raise NotImplementedError()

    @overload
    def jdbc(
        self, url: str, table: str, *, properties: Optional[Dict[str, str]] = None
    ) -> "DataFrame":
        ...

    @overload
    def jdbc(
        self,
        url: str,
        table: str,
        column: str,
        lowerBound: Union[int, str],
        upperBound: Union[int, str],
        numPartitions: int,
        *,
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        ...

    @overload
    def jdbc(
        self,
        url: str,
        table: str,
        *,
        predicates: List[str],
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        ...

    def jdbc(
        self,
        url: str,
        table: str,
        column: Optional[str] = None,
        lowerBound: Optional[Union[int, str]] = None,
        upperBound: Optional[Union[int, str]] = None,
        numPartitions: Optional[int] = None,
        predicates: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> "DataFrame":
        """
        Construct a :class:`DataFrame` representing the database table named ``table``
        accessible via JDBC URL ``url`` and connection ``properties``.

        Partitions of the table will be retrieved in parallel if either ``column`` or
        ``predicates`` is specified. ``lowerBound``, ``upperBound`` and ``numPartitions``
        is needed when ``column`` is specified.

        If both ``column`` and ``predicates`` are specified, ``column`` will be used.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        table : str
            the name of the table
        column : str, optional
            alias of ``partitionColumn`` option. Refer to ``partitionColumn`` in
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.
        predicates : list, optional
            a list of expressions suitable for inclusion in WHERE clauses;
            each one defines one partition of the :class:`DataFrame`
        properties : dict, optional
            a dictionary of JDBC database connection arguments. Normally at
            least properties "user" and "password" with their corresponding values.
            For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.

        Returns
        -------
        :class:`DataFrame`
        """
        raise NotImplementedError()


class DataFrameWriter(OptionUtils):
    """
    Interface used to write a :class:`DataFrame` to external storage systems
    (e.g. file systems, key-value stores, etc). Use :attr:`DataFrame.write`
    to access this.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: "DataFrame"):
        self._df = df
        self._spark = df.sparkSession
        self._mode = "error"
        self._format = self._spark.conf.get("spark.sql.sources.default")
        self._options = {}
        self._part_cols = []
        self._bucket_by = ()
        self._sort_by = []

    def mode(self, saveMode: Optional[str]) -> "DataFrameWriter":
        """Specifies the behavior when data or table already exists.

        Options include:

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Examples
        --------
        Raise an error when writing to an existing path.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     spark.createDataFrame(
        ...         [{"age": 80, "name": "Xinrong Meng"}]
        ...     ).write.mode("error").format("parquet").save(d) # doctest: +SKIP
        Traceback (most recent call last):
            ...
        ...AnalysisException: ...

        Write a Parquet file back with various options, and read it back.

        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Overwrite the path with a new Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Append another DataFrame into the Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 120, "name": "Takuya Ueshin"}]
        ...     ).write.mode("append").format("parquet").save(d)
        ...
        ...     # Append another DataFrame into the Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 140, "name": "Haejoon Lee"}]
        ...     ).write.mode("ignore").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).show()
        +---+-------------+
        |age|         name|
        +---+-------------+
        |120|Takuya Ueshin|
        |100| Hyukjin Kwon|
        +---+-------------+
        """
        self._mode = saveMode
        return self

    def format(self, source: str) -> "DataFrameWriter":
        """Specifies the underlying output data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, e.g. 'json', 'parquet'.

        Examples
        --------
        >>> spark.range(1).write.format('parquet')
        <...readwriter.DataFrameWriter object ...>

        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format('parquet').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self._format = source
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds an output option for the underlying data source.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        key : str
            The key for the option to set.
        value
            The value for the option to set.

        Examples
        --------
        >>> spark.range(1).write.option("key", "value")
        <...readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' with writing a CSV file.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     df = spark.createDataFrame([(100, None)], "age INT, name STRING")
        ...     df.write.option("nullValue", "Hyukjin Kwon").mode("overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame.
        ...     spark.read.schema(df.schema).format('csv').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """

        self._options[key] = to_str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriter":
        """
        Adds output options for the underlying data source.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        **options : dict
            The dictionary of string keys and primitive-type values.

        Examples
        --------
        >>> spark.range(1).write.options(key="value")
        <...readwriter.DataFrameWriter object ...>

        Specify options in a dictionary.

        >>> spark.range(1).write.options(**{"k1": "v1", "k2": "v2"})
        <...readwriter.DataFrameWriter object ...>

        Specify the option 'nullValue' and 'header' with writing a CSV file.

        >>> from polarspark.sql.types import StructType,StructField, StringType, IntegerType
        >>> schema = StructType([
        ...     StructField("age",IntegerType(),True),
        ...     StructField("name",StringType(),True),
        ... ])
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file with 'nullValue' option set to 'Hyukjin Kwon',
        ...     # and 'header' option set to `True`.
        ...     df = spark.createDataFrame([(100, None)], schema=schema)
        ...     df.write.options(nullValue="Hyukjin Kwon", header=True).mode(
        ...         "overwrite").format("csv").save(d)
        ...
        ...     # Read the CSV file as a DataFrame.
        ...     spark.read.option("header", True).format('csv').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        for k in options:
            self._options[k] = to_str(options[k])
        return self

    @overload
    def partitionBy(self, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def partitionBy(self, *cols: List[str]) -> "DataFrameWriter":
        ...

    def partitionBy(self, *cols: Union[str, List[str]]) -> "DataFrameWriter":
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str or list
            name of columns

        Examples
        --------
        Write a DataFrame into a Parquet file in a partitioned manner, and read it back.

        >>> import tempfile
        >>> import os
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file in a partitioned manner.
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}, {"age": 120, "name": "Ruifeng Zheng"}]
        ...     ).write.partitionBy("name").mode("overwrite").format("parquet").save(d)
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.parquet(d).sort("age").show()
        ...
        ...     # Read one partition as a DataFrame.
        ...     spark.read.parquet(f"{d}{os.path.sep}name=Hyukjin Kwon").show()
        +---+-------------+
        |age|         name|
        +---+-------------+
        |100| Hyukjin Kwon|
        |120|Ruifeng Zheng|
        +---+-------------+
        +---+
        |age|
        +---+
        |100|
        +---+
        """
        if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
            cols = cols[0]  # type: ignore[assignment]
        self._part_cols = cols
        return self

    @overload
    def bucketBy(self, numBuckets: int, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def bucketBy(self, numBuckets: int, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def bucketBy(
        self, numBuckets: int, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        """Buckets the output by the given columns. If specified,
        the output is laid out on the file system similar to Hive's bucketing scheme,
        but with a different bucket hash function and is not compatible with Hive's bucketing.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        numBuckets : int
            the number of buckets to save
        col : str, list or tuple
            a name of a column, or a list of names.
        cols : str
            additional names (optional). If `col` is a list it should be empty.

        Notes
        -----
        Applicable for file-based data sources in combination with
        :py:meth:`DataFrameWriter.saveAsTable`.

        Examples
        --------
        Write a DataFrame into a Parquet file in a buckted manner, and read it back.

        >>> from polarspark.sql.functions import input_file_name
        >>> # Write a DataFrame into a Parquet file in a bucketed manner.
        ... _ = spark.sql("DROP TABLE IF EXISTS bucketed_table")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.bucketBy(2, "name").mode("overwrite").saveAsTable("bucketed_table")
        >>> # Read the Parquet file as a DataFrame.
        ... spark.read.table("bucketed_table").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE bucketed_table")
        """
        if not isinstance(numBuckets, int):
            raise PySparkTypeError(
                error_class="NOT_INT",
                message_parameters={
                    "arg_name": "numBuckets",
                    "arg_type": type(numBuckets).__name__,
                },
            )

        if isinstance(col, (list, tuple)):
            if cols:
                raise PySparkValueError(
                    error_class="CANNOT_SET_TOGETHER",
                    message_parameters={
                        "arg_list": f"`col` of type {type(col).__name__} and `cols`",
                    },
                )

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        for c in cols:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={
                        "arg_name": "cols",
                        "arg_type": type(c).__name__,
                    },
                )
        if not isinstance(col, str):
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_STR",
                message_parameters={
                    "arg_name": "col",
                    "arg_type": type(col).__name__,
                },
            )

        self._bucket_by = (numBuckets, col, cols)
        return self

    @overload
    def sortBy(self, col: str, *cols: str) -> "DataFrameWriter":
        ...

    @overload
    def sortBy(self, col: TupleOrListOfString) -> "DataFrameWriter":
        ...

    def sortBy(
        self, col: Union[str, TupleOrListOfString], *cols: Optional[str]
    ) -> "DataFrameWriter":
        """Sorts the output in each bucket by the given columns on the file system.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        col : str, tuple or list
            a name of a column, or a list of names.
        cols : str
            additional names (optional). If `col` is a list it should be empty.

        Examples
        --------
        Write a DataFrame into a Parquet file in a sorted-buckted manner, and read it back.

        >>> from polarspark.sql.functions import input_file_name
        >>> # Write a DataFrame into a Parquet file in a sorted-bucketed manner.
        ... _ = spark.sql("DROP TABLE IF EXISTS sorted_bucketed_table")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.bucketBy(1, "name").sortBy("age").mode(
        ...     "overwrite").saveAsTable("sorted_bucketed_table")
        >>> # Read the Parquet file as a DataFrame.
        ... spark.read.table("sorted_bucketed_table").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE sorted_bucketed_table")
        """
        if isinstance(col, (list, tuple)):
            if cols:
                raise PySparkValueError(
                    error_class="CANNOT_SET_TOGETHER",
                    message_parameters={
                        "arg_list": f"`col` of type {type(col).__name__} and `cols`",
                    },
                )

            col, cols = col[0], col[1:]  # type: ignore[assignment]

        for c in cols:
            if not isinstance(c, str):
                raise PySparkTypeError(
                    error_class="NOT_LIST_OF_STR",
                    message_parameters={
                        "arg_name": "cols",
                        "arg_type": type(c).__name__,
                    },
                )
        if not isinstance(col, str):
            raise PySparkTypeError(
                error_class="NOT_LIST_OF_STR",
                message_parameters={
                    "arg_name": "col",
                    "arg_type": type(col).__name__,
                },
            )

        self._sort_by = [col, *cols]
        return self

    def save(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """Saves the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, optional
            the path in a Hadoop supported file system
        format : str, optional
            the format used to save
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : list, optional
            names of partitioning columns
        **options : dict
            all other string options

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format('json').load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        if mode:
            self.mode(mode)
        if options:
            self.options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        for ldf in self._df._gather():  # noqa
            _save(ldf, path, self._format, self._mode, self._part_cols, self._options)

    def insertInto(self, tableName: str, overwrite: Optional[bool] = None) -> None:
        """Inserts the content of the :class:`DataFrame` to the specified table.

        It requires that the schema of the :class:`DataFrame` is the same as the
        schema of the table.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        overwrite : bool, optional
            If true, overwrites existing data. Disabled by default

        Notes
        -----
        Unlike :meth:`DataFrameWriter.saveAsTable`, :meth:`DataFrameWriter.insertInto` ignores
        the column names and just uses position-based resolution.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tblA")
        >>> df = spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... )
        >>> df.write.saveAsTable("tblA")

        Insert the data into 'tblA' table but with different column names.

        >>> df.selectExpr("age AS col1", "name AS col2").write.insertInto("tblA")
        >>> spark.read.table("tblA").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        if overwrite is not None:
            self.mode("overwrite" if overwrite else "append")
        self._jwrite.insertInto(tableName)

    def saveAsTable(
        self,
        name: str,
        format: Optional[str] = None,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        **options: "OptionalPrimitiveType",
    ) -> None:
        """Saves the content of the :class:`DataFrame` as the specified table.

        In the case the table already exists, behavior of this function depends on the
        save mode, specified by the `mode` function (default to throwing an exception).
        When `mode` is `Overwrite`, the schema of the :class:`DataFrame` does not need to be
        the same as that of the existing table.

        * `append`: Append contents of this :class:`DataFrame` to existing data.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        When `mode` is `Append`, if there is an existing table, we will use the format and
        options of the existing table. The column order in the schema of the :class:`DataFrame`
        doesn't need to be the same as that of the existing table. Unlike
        :meth:`DataFrameWriter.insertInto`, :meth:`DataFrameWriter.saveAsTable` will use the
        column names to find the correct column positions.

        Parameters
        ----------
        name : str
            the table name
        format : str, optional
            the format used to save
        mode : str, optional
            one of `append`, `overwrite`, `error`, `errorifexists`, `ignore` \
            (default: error)
        partitionBy : str or list
            names of partitioning columns
        **options : dict
            all other string options

        Examples
        --------
        Creates a table from a DataFrame, and read it back.

        >>> _ = spark.sql("DROP TABLE IF EXISTS tblA")
        >>> spark.createDataFrame([
        ...     (100, "Hyukjin Kwon"), (120, "Hyukjin Kwon"), (140, "Haejoon Lee")],
        ...     schema=["age", "name"]
        ... ).write.saveAsTable("tblA")
        >>> spark.read.name("tblA").sort("age").show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        |120|Hyukjin Kwon|
        |140| Haejoon Lee|
        +---+------------+
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        self.mode(mode).options(**options)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)

        cat = self._spark.catalog
        names = parse_table_name(name)
        default_spark_path = cat.DEFAULT_SPARK_PATH
        path = pathlib.Path(default_spark_path).joinpath(names.table)

        # Save
        self.save(str(path.absolute()))

        if not cat._cat.get_table(name):  # noqa
            cat.createTable(names.table, str(path.absolute()))
        # TODO: Add mode support for inserts and overwrites

    def json(
        self,
        path: str,
        mode: Optional[str] = None,
        compression: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        lineSep: Optional[str] = None,
        encoding: Optional[str] = None,
        ignoreNullFields: Optional[Union[bool, str]] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in JSON format
        (`JSON Lines text format or newline-delimited JSON <http://jsonlines.org/>`_) at the
        specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a JSON file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a JSON file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.json(d, mode="overwrite")
        ...
        ...     # Read the JSON file as a DataFrame.
        ...     spark.read.format("json").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        self._set_opts(
            compression=compression,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            lineSep=lineSep,
            encoding=encoding,
            ignoreNullFields=ignoreNullFields,
        )
        # self._jwrite.json(path)
        self.save(path=path, format="json")

    def parquet(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in Parquet format at the specified path.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : str or list, optional
            names of partitioning columns

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a Parquet file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a Parquet file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.parquet(d, mode="overwrite")
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format("parquet").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        self.save(path=path, format="parquet")

    def text(
        self, path: str, compression: Optional[str] = None, lineSep: Optional[str] = None
    ) -> None:
        """Saves the content of the DataFrame in a text file at the specified path.
        The text files will be encoded as UTF-8.

        .. versionadded:: 1.6.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        The DataFrame must have only one column that is of string type.
        Each row becomes a new line in the output file.

        Examples
        --------
        Write a DataFrame into a text file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a text file
        ...     df = spark.createDataFrame([("a",), ("b",), ("c",)], schema=["alphabets"])
        ...     df.write.mode("overwrite").text(d)
        ...
        ...     # Read the text file as a DataFrame.
        ...     spark.read.schema(df.schema).format("text").load(d).sort("alphabets").show()
        +---------+
        |alphabets|
        +---------+
        |        a|
        |        b|
        |        c|
        +---------+
        """
        self._set_opts(compression=compression, lineSep=lineSep)
        # self._jwrite.text(path)
        self.save(path=path, format="text")

    def csv(
        self,
        path: str,
        mode: Optional[str] = None,
        compression: Optional[str] = None,
        sep: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        header: Optional[Union[bool, str]] = None,
        nullValue: Optional[str] = None,
        escapeQuotes: Optional[Union[bool, str]] = None,
        quoteAll: Optional[Union[bool, str]] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        ignoreLeadingWhiteSpace: Optional[Union[bool, str]] = None,
        ignoreTrailingWhiteSpace: Optional[Union[bool, str]] = None,
        charToEscapeQuoteEscaping: Optional[str] = None,
        encoding: Optional[str] = None,
        emptyValue: Optional[str] = None,
        lineSep: Optional[str] = None,
    ) -> None:
        r"""Saves the content of the :class:`DataFrame` in CSV format at the specified path.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a CSV file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a CSV file
        ...     df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
        ...     df.write.csv(d, mode="overwrite")
        ...
        ...     # Read the CSV file as a DataFrame with 'nullValue' option set to 'Hyukjin Kwon'.
        ...     spark.read.schema(df.schema).format("csv").option(
        ...         "nullValue", "Hyukjin Kwon").load(d).show()
        +---+----+
        |age|name|
        +---+----+
        |100|NULL|
        +---+----+
        """
        self.mode(mode)
        self._set_opts(
            compression=compression,
            sep=sep,
            quote=quote,
            escape=escape,
            header=header,
            nullValue=nullValue,
            escapeQuotes=escapeQuotes,
            quoteAll=quoteAll,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            ignoreLeadingWhiteSpace=ignoreLeadingWhiteSpace,
            ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
            charToEscapeQuoteEscaping=charToEscapeQuoteEscaping,
            encoding=encoding,
            emptyValue=emptyValue,
            lineSep=lineSep,
        )
        # self._jwrite.csv(path)
        self.save(path=path, format="csv")

    def xml(
        self,
        path: str,
        rowTag: Optional[str] = None,
        mode: Optional[str] = None,
        attributePrefix: Optional[str] = None,
        valueTag: Optional[str] = None,
        rootTag: Optional[str] = None,
        declaration: Optional[bool] = None,
        arrayElementName: Optional[str] = None,
        nullValue: Optional[str] = None,
        dateFormat: Optional[str] = None,
        timestampFormat: Optional[str] = None,
        compression: Optional[str] = None,
        encoding: Optional[str] = None,
    ) -> None:
        r"""Saves the content of the :class:`DataFrame` in XML format at the specified path.

        .. versionadded:: 4.0.0

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a XML file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a XML file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").option("rowTag", "person").xml(d)
        ...
        ...     # Read the XML file as a DataFrame.
        ...     spark.read.option("rowTag", "person").format("xml").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        self._set_opts(
            rowTag=rowTag,
            attributePrefix=attributePrefix,
            valueTag=valueTag,
            rootTag=rootTag,
            declaration=declaration,
            arrayElementName=arrayElementName,
            nullValue=nullValue,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            compression=compression,
            encoding=encoding,
        )
        raise NotImplementedError()

    def orc(
        self,
        path: str,
        mode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` in ORC format at the specified path.

        .. versionadded:: 1.5.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        partitionBy : str or list, optional
            names of partitioning columns

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Examples
        --------
        Write a DataFrame into a ORC file and read it back.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a ORC file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.orc(d, mode="overwrite")
        ...
        ...     # Read the Parquet file as a DataFrame.
        ...     spark.read.format("orc").load(d).show()
        +---+------------+
        |age|        name|
        +---+------------+
        |100|Hyukjin Kwon|
        +---+------------+
        """
        self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        self._set_opts(compression=compression)
        raise NotImplementedError()

    def jdbc(
        self,
        url: str,
        table: str,
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> None:
        """Saves the content of the :class:`DataFrame` to an external database table via JDBC.

        .. versionadded:: 1.4.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Parameters
        ----------
        table : str
            Name of the table in the external database.
        mode : str, optional
            specifies the behavior of the save operation when data already exists.

            * ``append``: Append contents of this :class:`DataFrame` to existing data.
            * ``overwrite``: Overwrite existing data.
            * ``ignore``: Silently ignore this operation if data already exists.
            * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
                exists.
        properties : dict
            a dictionary of JDBC database connection arguments. Normally at
            least properties "user" and "password" with their corresponding values.
            For example { 'user' : 'SYSTEM', 'password' : 'mypassword' }

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option>`_
            for the version you use.

            .. # noqa

        Notes
        -----
        Don't create too many partitions in parallel on a large cluster;
        otherwise Spark might crash your external database systems.
        """
        if properties is None:
            properties = dict()

        assert self._spark._sc._gateway is not None
        jprop = JavaClass(
            "java.util.Properties",
            self._spark._sc._gateway._gateway_client,
        )()
        for k in properties:
            jprop.setProperty(k, properties[k])
        self.mode(mode)
        raise NotImplementedError()


def _save(
    # self,
    ldf: pl.LazyFrame,
    path: Optional[str] = None,
    format: Optional[str] = None,  # noqa
    mode: Optional[str] = None,
    # TODO: Add partitioning
    partitionBy: Optional[Union[str, List[str]]] = None,
    options: Optional[dict] = None,
):
    assert format is not None, "Format must be specified"

    # Create dir with target file name
    p = Path(path or options.get("path"))
    if format != "delta":
        path = p / f"part-00000-{uuid.uuid4()}-c000.{p.suffix or format}"
    else:
        path = p

    if mode:
        if p.exists() and mode == "overwrite":
            shutil.rmtree(p)
        elif p.exists() and mode == "error":
            raise PySparkRuntimeError(
                error_class="PATH_ALREADY_EXISTS",
                message_parameters={"path": str(p)},
            )
    if not p.exists():
        p.mkdir(parents=True)

    if path is None:
        pass
        # FIX check what is default path or table?
        # self._jwrite.save()
    else:
        writers = {
            "csv": ldf.sink_csv,
            "json": ldf.sink_ndjson,
            "parquet": ldf.sink_parquet,
            "delta": ldf.collect().write_delta,
            "excel": ldf.collect().write_excel,
        }
        write = writers.get(format)
        if write is None:
            raise PySparkRuntimeError(f"Format {format} not supported")
        write(path)  # , **options)


class DataFrameWriterV2:
    """
    Interface used to write a class:`polarspark.sql.dataframe.DataFrame`
    to external storage using the v2 API.

    .. versionadded:: 3.1.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    def __init__(self, df: "DataFrame", table: str):
        self._df = df
        self._spark = df.sparkSession
        self._jwriter = df._jdf.writeTo(table)

    @since(3.1)
    def using(self, provider: str) -> "DataFrameWriterV2":
        """
        Specifies a provider for the underlying output data source.
        Spark's default catalog supports "parquet", "json", etc.
        """
        self._jwriter.using(provider)
        return self

    @since(3.1)
    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        """
        Add a write option.
        """
        self._jwriter.option(key, to_str(value))
        return self

    @since(3.1)
    def options(self, **options: "OptionalPrimitiveType") -> "DataFrameWriterV2":
        """
        Add write options.
        """
        options = {k: to_str(v) for k, v in options.items()}
        self._jwriter.options(options)
        return self

    @since(3.1)
    def tableProperty(self, property: str, value: str) -> "DataFrameWriterV2":
        """
        Add table property.
        """
        self._jwriter.tableProperty(property, value)
        return self

    @since(3.1)
    def partitionedBy(self, col: Column, *cols: Column) -> "DataFrameWriterV2":
        """
        Partition the output table created by `create`, `createOrReplace`, or `replace` using
        the given columns or transforms.

        When specified, the table data will be stored by these values for efficient reads.

        For example, when a table is partitioned by day, it may be stored
        in a directory layout like:

        * `table/day=2019-06-01/`
        * `table/day=2019-06-02/`

        Partitioning is one of the most widely used techniques to optimize physical data layout.
        It provides a coarse-grained index for skipping unnecessary data reads when queries have
        predicates on the partitioned columns. In order for partitioning to work well, the number
        of distinct values in each column should typically be less than tens of thousands.

        `col` and `cols` support only the following functions:

        * :py:func:`polarspark.sql.functions.years`
        * :py:func:`polarspark.sql.functions.months`
        * :py:func:`polarspark.sql.functions.days`
        * :py:func:`polarspark.sql.functions.hours`
        * :py:func:`polarspark.sql.functions.bucket`

        """
        col = _to_java_column(col)
        cols = _to_seq(self._spark._sc, [_to_java_column(c) for c in cols])
        self._jwriter.partitionedBy(col, cols)
        return self

    @since(3.1)
    def create(self) -> None:
        """
        Create a new table from the contents of the data frame.

        The new table's schema, partition layout, properties, and other configuration will be
        based on the configuration set on this writer.
        """
        self._jwriter.create()

    @since(3.1)
    def replace(self) -> None:
        """
        Replace an existing table with the contents of the data frame.

        The existing table's schema, partition layout, properties, and other configuration will be
        replaced with the contents of the data frame and the configuration set on this writer.
        """
        self._jwriter.replace()

    @since(3.1)
    def createOrReplace(self) -> None:
        """
        Create a new table or replace an existing table with the contents of the data frame.

        The output table's schema, partition layout, properties,
        and other configuration will be based on the contents of the data frame
        and the configuration set on this writer.
        If the table exists, its configuration and data will be replaced.
        """
        self._jwriter.createOrReplace()

    @since(3.1)
    def append(self) -> None:
        """
        Append the contents of the data frame to the output table.
        """
        self._jwriter.append()

    @since(3.1)
    def overwrite(self, condition: Column) -> None:
        """
        Overwrite rows matching the given filter condition with the contents of the data frame in
        the output table.
        """
        condition = _to_java_column(condition)
        self._jwriter.overwrite(condition)

    @since(3.1)
    def overwritePartitions(self) -> None:
        """
        Overwrite all partition for which the data frame contains at least one row with the contents
        of the data frame in the output table.

        This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
        partitions dynamically depending on the contents of the data frame.
        """
        self._jwriter.overwritePartitions()


def _test() -> None:
    import doctest
    import os
    import py4j
    from polarspark.context import SparkContext
    from polarspark.sql import SparkSession
    import polarspark.sql.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = polarspark.sql.readwriter.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        polarspark.sql.readwriter,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
