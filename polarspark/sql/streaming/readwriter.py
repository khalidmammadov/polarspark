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
import itertools
import sys
import time
import uuid
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Event
from collections.abc import Iterator
from typing import cast, overload, Any, Callable, List, Optional, TYPE_CHECKING, Union, Generator
from dataclasses import dataclass

from polarspark.sql.streaming.file_watcher import watch_files

import polars as pl
from polarspark.sql.readwriter import OptionUtils, to_str, _save
from polarspark.sql.streaming.query import StreamingQuery
from polarspark.sql.types import Row, StructType, DataType
from polarspark.sql.utils import NO_INPUT, ForeachBatchFunction
from polarspark.errors import (
    PySparkTypeError,
    PySparkValueError,
    PySparkAttributeError,
    PySparkRuntimeError,
    AnalysisException,
)

if TYPE_CHECKING:
    from polarspark.sql.session import SparkSession

    # from polarspark.sql._typing import SupportsProcess, OptionalPrimitiveType
    from polarspark.sql.dataframe import DataFrame

__all__ = ["DataStreamReader", "DataStreamWriter"]


class DataStreamReader(OptionUtils):
    """
    Interface used to load a streaming :class:`DataFrame <pyspark.sql.DataFrame>` from external
    storage systems (e.g. file systems, key-value stores, etc).
    Use :attr:`SparkSession.readStream <pyspark.sql.SparkSession.readStream>` to access this.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Notes
    -----
    This API is evolving.

    Examples
    --------
    >>> spark.readStream
    <...streaming.readwriter.DataStreamReader object ...>

    The example below uses Rate source that generates rows continuously.
    After that, we operate a modulo by 3, and then writes the stream out to the console.
    The streaming query stops in 3 seconds.

    >>> import time
    >>> df = spark.readStream.format("rate").load()
    >>> df = df.selectExpr("value % 3 as v")
    >>> q = df.writeStream.format("console").start()
    >>> time.sleep(3)
    >>> q.stop()
    """

    def __init__(self, spark: "SparkSession") -> None:
        self._spark = spark

        self._format = None
        self._options = {}
        self._schema = None

    def format(self, source: str) -> "DataStreamReader":
        """Specifies the input data source format.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            name of the data source, e.g. 'json', 'parquet'.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> spark.readStream.format("text")
        <...streaming.readwriter.DataStreamReader object ...>

        This API allows to configure other sources to read. The example below writes a small text
        file, and reads it back via Text source.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary text file to read it.
        ...     spark.createDataFrame(
        ...         [("hello",), ("this",)]).write.mode("overwrite").format("text").save(d)
        ...
        ...     # Start a streaming query to read the text file.
        ...     q = spark.readStream.format("text").load(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        self._format = source
        return self

    def schema(self, schema: Union[StructType, str]) -> "DataStreamReader":
        """Specifies the input schema.

        Some data sources (e.g. JSON) can infer the input schema automatically from data.
        By specifying the schema here, the underlying data source can skip the schema
        inference step, and thus speed up data loading.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        schema : :class:`pyspark.sql.types.StructType` or str
            a :class:`pyspark.sql.types.StructType` object or a DDL-formatted string
            (For example ``col0 INT, col1 DOUBLE``).

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> from pyspark.sql.types import StructField, StructType, StringType
        >>> spark.readStream.schema(StructType([StructField("data", StringType(), True)]))
        <...streaming.readwriter.DataStreamReader object ...>
        >>> spark.readStream.schema("col0 INT, col1 DOUBLE")
        <...streaming.readwriter.DataStreamReader object ...>

        The example below specifies a different schema to CSV file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Start a streaming query to read the CSV file.
        ...     spark.readStream.schema("col0 INT, col1 STRING").format("csv").load(d).printSchema()
        root
         |-- col0: integer (nullable = true)
         |-- col1: string (nullable = true)
        """

        if not isinstance(schema, (StructType, str)):
            raise PySparkTypeError(
                error_class="NOT_STR_OR_STRUCT",
                message_parameters={"arg_name": "schema", "arg_type": type(schema).__name__},
            )
        self._schema = schema
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataStreamReader":
        """Adds an input option for the underlying data source.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> spark.readStream.option("x", 1)
        <...streaming.readwriter.DataStreamReader object ...>

        The example below specifies 'rowsPerSecond' option to Rate source in order to generate
        10 rows every second.

        >>> import time
        >>> q = spark.readStream.format(
        ...     "rate").option("rowsPerSecond", 10).load().writeStream.format("console").start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        self._options[key] = to_str(value)
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataStreamReader":
        """Adds input options for the underlying data source.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> spark.readStream.options(x="1", y=2)
        <...streaming.readwriter.DataStreamReader object ...>

        Specify options in a dictionary.

        >>> spark.readStream.options(**{"k1": "v1", "k2": "v2"})
        <...streaming.readwriter.DataStreamReader object ...>

        The example below specifies 'rowsPerSecond' and 'numPartitions' options to
        Rate source in order to generate 10 rows with 10 partitions every second.

        >>> import time
        >>> q = spark.readStream.format("rate").options(
        ...    rowsPerSecond=10, numPartitions=10
        ... ).load().writeStream.format("console").start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        if options:
            self._options = self._options.update(options)
        return self

    def load(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema: Optional[Union[StructType, str]] = None,
        **options: "OptionalPrimitiveType",
    ) -> "DataFrame":
        """Loads a data stream from a data source and returns it as a
        :class:`DataFrame <pyspark.sql.DataFrame>`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, optional
            optional string for file-system backed data sources.
        format : str, optional
            optional string for format of the data source. Default to 'parquet'.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).
        **options : dict
            all other string options

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Load a data stream from a temporary JSON file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary JSON file to read it.
        ...     spark.createDataFrame(
        ...         [(100, "Hyukjin Kwon"),], ["age", "name"]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Start a streaming query to read the JSON file.
        ...     q = spark.readStream.schema(
        ...         "age INT, name STRING"
        ...     ).format("json").load(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        if format is not None:
            self.format(format)
        if schema is not None:
            self.schema(schema)
        self.options(**options)

        _path = path or self._options.get("path")
        if _path is not None:
            return self._streaming_df_from_path(_path)
        elif self._format == "rate":
            return self._streaming_df_from_rate()
        else:
            raise NotImplementedError("This stream config not supported")

    def _streaming_df_from_rate(self) -> "DataFrame":
        from polarspark.sql.dataframe import DataFrame

        def ldf_generator() -> Generator[pl.LazyFrame, None, None]:
            rps = self._options.get("rowsPerSecond")
            i = 0
            step = int(rps) if rps else 1
            while True:
                if rps:
                    yield pl.LazyFrame({"value": list(range(i, i + step))})
                    time.sleep(1)
                else:
                    yield pl.LazyFrame({"value": [i]})
                i = i + step

        return DataFrame(None, ldf_generator, self._spark, is_streaming=True, origin=self)

    def _streaming_df_from_path(self, path) -> "DataFrame":
        from polarspark.sql.dataframe import DataFrame

        if type(path) != str or len(path.strip()) == 0:
            raise PySparkValueError(
                error_class="VALUE_NOT_NON_EMPTY_STR",
                message_parameters={"arg_name": "path", "arg_value": str(path)},
            )

        def ldf_generator() -> Generator[pl.LazyFrame, None, None]:
            df_reader = self._spark.read.options(**self._options).format(self._format)  # noqa
            if self._schema:
                df_reader = df_reader.schema(self._schema)

            if self._format in ["text", "csv", "json", "parquet", "excel"]:
                for file in watch_files(path):
                    if file and not isinstance(file, Path):
                        yield NO_INPUT
                        continue
                    # TODO: Fix this so you get LDF directly rather than
                    # evaluating using DF load which does eager schema
                    # resolution
                    df = df_reader.load(str(file))
                    yield df._gather_first()  # noqa
            elif self._format == "delta":  # TODO: use history
                df = df_reader.load(path)
                yield df._gather_first()  # noqa
            else:
                raise NotImplementedError(f"Source {self._format} is not supported.")

        return DataFrame(None, ldf_generator, self._spark, is_streaming=True, origin=self)

    def json(
        self,
        path: str,
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
        locale: Optional[str] = None,
        dropFieldIfAllNull: Optional[Union[bool, str]] = None,
        encoding: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        allowNonNumericNumbers: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads a JSON file stream and returns the results as a :class:`DataFrame`.

        `JSON Lines <http://jsonlines.org/>`_ (newline-delimited JSON) is supported by default.
        For JSON (one record per file), set the ``multiLine`` parameter to ``true``.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            string represents path to the JSON dataset,
            or RDD of Strings storing JSON objects.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option>`_
            in the version you use.

            .. # noqa

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Load a data stream from a temporary JSON file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary JSON file to read it.
        ...     spark.createDataFrame(
        ...         [(100, "Hyukjin Kwon"),], ["age", "name"]
        ...     ).write.mode("overwrite").format("json").save(d)
        ...
        ...     # Start a streaming query to read the JSON file.
        ...     q = spark.readStream.schema(
        ...         "age INT, name STRING"
        ...     ).json(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
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
            locale=locale,
            dropFieldIfAllNull=dropFieldIfAllNull,
            encoding=encoding,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            allowNonNumericNumbers=allowNonNumericNumbers,
        )
        if isinstance(path, str):
            return self._df(self._jreader.json(path))
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def orc(
        self,
        path: str,
        mergeSchema: Optional[bool] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """Loads a ORC file stream, returning the result as a :class:`DataFrame`.

        .. versionadded:: 2.3.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option>`_
            in the version you use.

            .. # noqa

        Examples
        --------
        Load a data stream from a temporary ORC file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary ORC file to read it.
        ...     spark.range(10).write.mode("overwrite").format("orc").save(d)
        ...
        ...     # Start a streaming query to read the ORC file.
        ...     q = spark.readStream.schema("id LONG").orc(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
        )
        if isinstance(path, str):
            return self._df(self._jreader.orc(path))
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def parquet(
        self,
        path: str,
        mergeSchema: Optional[bool] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        datetimeRebaseMode: Optional[Union[bool, str]] = None,
        int96RebaseMode: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads a Parquet file stream, returning the result as a :class:`DataFrame`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str
            the path in any Hadoop supported file system

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option>`_.
            in the version you use.

            .. # noqa

        Examples
        --------
        Load a data stream from a temporary Parquet file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary Parquet file to read it.
        ...     spark.range(10).write.mode("overwrite").format("parquet").save(d)
        ...
        ...     # Start a streaming query to read the Parquet file.
        ...     q = spark.readStream.schema(
        ...         "id LONG").parquet(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        self._set_opts(
            mergeSchema=mergeSchema,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            datetimeRebaseMode=datetimeRebaseMode,
            int96RebaseMode=int96RebaseMode,
        )
        if isinstance(path, str):
            return self._df(self._jreader.parquet(path))
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def text(
        self,
        path: str,
        wholetext: bool = False,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
    ) -> "DataFrame":
        """
        Loads a text file stream and returns a :class:`DataFrame` whose schema starts with a
        string column named "value", and followed by partitioned columns if there
        are any.
        The text files must be encoded as UTF-8.

        By default, each line in the text file is a new row in the resulting DataFrame.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s).

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option>`_
            in the version you use.

            .. # noqa

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Load a data stream from a temporary text file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary text file to read it.
        ...     spark.createDataFrame(
        ...         [("hello",), ("this",)]).write.mode("overwrite").format("text").save(d)
        ...
        ...     # Start a streaming query to read the text file.
        ...     q = spark.readStream.text(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        self._set_opts(
            wholetext=wholetext,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
        )
        if isinstance(path, str):
            return self._df(self._jreader.text(path))
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def csv(
        self,
        path: str,
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
        charToEscapeQuoteEscaping: Optional[Union[bool, str]] = None,
        enforceSchema: Optional[Union[bool, str]] = None,
        emptyValue: Optional[str] = None,
        locale: Optional[str] = None,
        lineSep: Optional[str] = None,
        pathGlobFilter: Optional[Union[bool, str]] = None,
        recursiveFileLookup: Optional[Union[bool, str]] = None,
        unescapedQuoteHandling: Optional[str] = None,
    ) -> "DataFrame":
        r"""Loads a CSV file stream and returns the result as a :class:`DataFrame`.

        This function will go through the input once to determine the input schema if
        ``inferSchema`` is enabled. To avoid going through the entire data once, disable
        ``inferSchema`` option or specify the schema explicitly using ``schema``.

        Parameters
        ----------
        path : str or list
            string, or list of strings, for input path(s).
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option>`_
            in the version you use.

            .. # noqa

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Load a data stream from a temporary CSV file.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a temporary text file to read it.
        ...     spark.createDataFrame([(1, "2"),]).write.mode("overwrite").format("csv").save(d)
        ...
        ...     # Start a streaming query to read the CSV file.
        ...     q = spark.readStream.schema(
        ...         "col0 INT, col1 STRING"
        ...     ).format("csv").load(d).writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
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
            enforceSchema=enforceSchema,
            emptyValue=emptyValue,
            locale=locale,
            lineSep=lineSep,
            pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup,
            unescapedQuoteHandling=unescapedQuoteHandling,
        )
        if isinstance(path, str):
            self.format("csv")
            return self.load(path)
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def xml(
        self,
        path: str,
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
        r"""Loads a XML file stream and returns the result as a :class:`DataFrame`.

        If the ``schema`` parameter is not specified, this function goes
        through the input once to determine the input schema.

        Parameters
        ----------
        path : str
            string for input path.
        schema : :class:`pyspark.sql.types.StructType` or str, optional
            an optional :class:`pyspark.sql.types.StructType` for the input schema
            or a DDL-formatted string (For example ``col0 INT, col1 DOUBLE``).

        .. versionadded:: 4.0.0

        Other Parameters
        ----------------
        Extra options
            For the extra options, refer to
            `Data Source Option <https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option>`_
            in the version you use.

            .. # noqa

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Write a DataFrame into a XML file and read it back.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Write a DataFrame into a XML file
        ...     spark.createDataFrame(
        ...         [{"age": 100, "name": "Hyukjin Kwon"}]
        ...     ).write.mode("overwrite").option("rowTag", "person").xml(d)
        ...
        ...     # Start a streaming query to read the XML file.
        ...     q = spark.readStream.schema(
        ...         "age INT, name STRING"
        ...     ).xml(d, rowTag="person").writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q.stop()
        """
        self._set_opts(
            rowTag=rowTag,
            schema=schema,
            excludeAttribute=excludeAttribute,
            attributePrefix=attributePrefix,
            valueTag=valueTag,
            ignoreSurroundingSpaces=ignoreSurroundingSpaces,
            rowValidationXSDPath=rowValidationXSDPath,
            ignoreNamespace=ignoreNamespace,
            wildcardColName=wildcardColName,
            encoding=encoding,
            inferSchema=inferSchema,
            nullValue=nullValue,
            dateFormat=dateFormat,
            timestampFormat=timestampFormat,
            mode=mode,
            columnNameOfCorruptRecord=columnNameOfCorruptRecord,
            multiLine=multiLine,
            samplingRatio=samplingRatio,
            locale=locale,
        )
        if isinstance(path, str):
            return self._df(self._jreader.xml(path))
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "path", "arg_type": type(path).__name__},
            )

    def table(self, tableName: str) -> "DataFrame":
        """Define a Streaming DataFrame on a Table. The DataSource corresponding to the table should
        support streaming mode.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        tableName : str
            string, for the name of the table.

        Returns
        -------
        :class:`DataFrame`

        Notes
        -----
        This API is evolving.

        Examples
        --------
        Load a data stream from a table.

        >>> import tempfile
        >>> import time
        >>> _ = spark.sql("DROP TABLE IF EXISTS my_table")
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Create a table with Rate source.
        ...     q1 = spark.readStream.format("rate").load().writeStream.toTable(
        ...         "my_table", checkpointLocation=d)
        ...
        ...     # Read the table back and print out in the console.
        ...     q2 = spark.readStream.name("my_table").writeStream.format("console").start()
        ...     time.sleep(3)
        ...     q1.stop()
        ...     q2.stop()
        ...     _ = spark.sql("DROP TABLE my_table")
        """
        if isinstance(tableName, str):
            tbl = self._spark.catalog._cat.get_table(tableName)
            return self.load(
                path=tbl.location,
                format=tbl.format,
                schema=",".join([" ".join(col) for col in tbl.columns]),
            )
        else:
            raise PySparkTypeError(
                error_class="NOT_STR",
                message_parameters={"arg_name": "tableName", "arg_type": type(tableName).__name__},
            )


@dataclass(frozen=True)
class Trigger:
    processingTime: Optional[str] = (None,)
    once: Optional[bool] = (None,)
    availableNow: Optional[bool] = (None,)


class DataStreamWriter:
    """
    Interface used to write a streaming :class:`DataFrame <pyspark.sql.DataFrame>` to external
    storage systems (e.g. file systems, key-value stores, etc).
    Use :attr:`DataFrame.writeStream <pyspark.sql.DataFrame.writeStream>`
    to access this.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Notes
    -----
    This API is evolving.

    Examples
    --------
    The example below uses Rate source that generates rows continuously.
    After that, we operate a modulo by 3, and then writes the stream out to the console.
    The streaming query stops in 3 seconds.

    >>> import time
    >>> df = spark.readStream.format("rate").load()
    >>> df = df.selectExpr("value % 3 as v")
    >>> q = df.writeStream.format("console").start()
    >>> time.sleep(3)
    >>> q.stop()
    """

    def __init__(self, df: "DataFrame") -> None:
        self._df = df
        self._spark = df.sparkSession

        self._format = None
        self._options = {}
        self._output_mode = None

        # Streaming
        self._query_id = str(uuid.uuid4())
        self._query_name = None
        self._foreach_func = None
        self._trigger = Trigger()
        self._partition_by_cols = None

        self._executor = ThreadPoolExecutor(max_workers=1)
        self._future: Optional[Future] = None

        self._active = Event()  # Thread safe due to GIL
        self._process_all_available = Event()

    def outputMode(self, outputMode: str) -> "DataStreamWriter":
        """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Options include:

        * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these are some updates
        * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.writeStream.outputMode('append')
        <...streaming.readwriter.DataStreamWriter object ...>

        The example below uses Complete mode that the entire aggregated counts are printed out.

        >>> import time
        >>> df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        >>> df = df.groupby().count()
        >>> q = df.writeStream.outputMode("complete").format("console").start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        if not outputMode or type(outputMode) != str or len(outputMode.strip()) == 0:
            raise PySparkValueError(
                error_class="VALUE_NOT_NON_EMPTY_STR",
                message_parameters={"arg_name": "outputMode", "arg_value": str(outputMode)},
            )
        # self._jwrite = self._jwrite.outputMode(outputMode)
        self._output_mode = outputMode
        return self

    def format(self, source: str) -> "DataStreamWriter":
        """Specifies the underlying output data source.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        source : str
            string, name of the data source, which for now can be 'parquet'.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.writeStream.format("text")
        <...streaming.readwriter.DataStreamWriter object ...>

        This API allows to configure the source to write. The example below writes a CSV
        file from Rate source in a streaming manner.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as cp:
        ...     df = spark.readStream.format("rate").load()
        ...     q = df.writeStream.format("csv").option("checkpointLocation", cp).start(d)
        ...     time.sleep(5)
        ...     q.stop()
        ...     spark.read.schema("timestamp TIMESTAMP, value STRING").csv(d).show()
        +...---------+-----+
        |...timestamp|value|
        +...---------+-----+
        ...
        """
        # self._jwrite = self._jwrite.format(source)
        self._format = source
        return self

    def option(self, key: str, value: "OptionalPrimitiveType") -> "DataStreamWriter":
        """Adds an output option for the underlying data source.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.writeStream.option("x", 1)
        <...streaming.readwriter.DataStreamWriter object ...>

        The example below specifies 'numRows' option to Console source in order to print
        3 rows for every batch.

        >>> import time
        >>> q = spark.readStream.format(
        ...     "rate").option("rowsPerSecond", 10).load().writeStream.format(
        ...         "console").option("numRows", 3).start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        self._options[key] = value
        return self

    def options(self, **options: "OptionalPrimitiveType") -> "DataStreamWriter":
        """Adds output options for the underlying data source.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.writeStream.option("x", 1)
        <...streaming.readwriter.DataStreamWriter object ...>

        Specify options in a dictionary.

        >>> df.writeStream.options(**{"k1": "v1", "k2": "v2"})
        <...streaming.readwriter.DataStreamWriter object ...>

        The example below specifies 'numRows' and 'truncate' options to Console source in order
        to print 3 rows for every batch without truncating the results.

        >>> import time
        >>> q = spark.readStream.format(
        ...     "rate").option("rowsPerSecond", 10).load().writeStream.format(
        ...         "console").options(numRows=3, truncate=False).start()
        >>> time.sleep(3)
        >>> q.stop()
        """
        self._options.update(options)
        return self

    @overload
    def partitionBy(self, *cols: str) -> "DataStreamWriter":
        ...

    @overload
    def partitionBy(self, __cols: List[str]) -> "DataStreamWriter":
        ...

    def partitionBy(self, *cols: str) -> "DataStreamWriter":  # type: ignore[misc]
        """Partitions the output by the given columns on the file system.

        If specified, the output is laid out on the file system similar
        to Hive's partitioning scheme.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        cols : str or list
            name of columns

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()
        >>> df.writeStream.partitionBy("value")
        <...streaming.readwriter.DataStreamWriter object ...>

        Partition-by timestamp column from Rate source.

        >>> import tempfile
        >>> import time
        >>> with tempfile.TemporaryDirectory() as d, tempfile.TemporaryDirectory() as cp:
        ...     df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        ...     q = df.writeStream.partitionBy(
        ...         "timestamp").format("parquet").option("checkpointLocation", cp).start(d)
        ...     time.sleep(5)
        ...     q.stop()
        ...     spark.read.schema(df.schema).parquet(d).show()
        +...---------+-----+
        |...timestamp|value|
        +...---------+-----+
        ...
        """
        self._partition_by_cols = cols
        return self

    def queryName(self, queryName: str) -> "DataStreamWriter":
        """Specifies the name of the :class:`StreamingQuery` that can be started with
        :func:`start`. This name must be unique among all the currently active queries
        in the associated SparkSession.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        queryName : str
            unique name for the query

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> import time
        >>> df = spark.readStream.format("rate").load()
        >>> q = df.writeStream.queryName("streaming_query").format("console").start()
        >>> q.stop()
        >>> q.name
        'streaming_query'
        """
        if not queryName or type(queryName) != str or len(queryName.strip()) == 0:
            raise PySparkValueError(
                error_class="VALUE_NOT_NON_EMPTY_STR",
                message_parameters={"arg_name": "queryName", "arg_value": str(queryName)},
            )
        self._query_name = queryName
        return self

    @overload
    def trigger(self, *, processingTime: str) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, once: bool) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, continuous: str) -> "DataStreamWriter":
        ...

    @overload
    def trigger(self, *, availableNow: bool) -> "DataStreamWriter":
        ...

    def trigger(
        self,
        *,
        processingTime: Optional[str] = None,
        once: Optional[bool] = None,
        continuous: Optional[str] = None,
        availableNow: Optional[bool] = None,
    ) -> "DataStreamWriter":
        """Set the trigger for the stream query. If this is not set it will run the query as fast
        as possible, which is equivalent to setting the trigger to ``processingTime='0 seconds'``.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        processingTime : str, optional
            a processing time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a microbatch query periodically based on the
            processing time. Only one trigger can be set.
        once : bool, optional
            if set to True, set a trigger that processes only one batch of data in a
            streaming query then terminates the query. Only one trigger can be set.
        continuous : str, optional
            a time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a continuous query with a given checkpoint
            interval. Only one trigger can be set.
        availableNow : bool, optional
            if set to True, set a trigger that processes all available data in multiple
            batches then terminates the query. Only one trigger can be set.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()

        Trigger the query for execution every 5 seconds

        >>> df.writeStream.trigger(processingTime='5 seconds')
        <...streaming.readwriter.DataStreamWriter object ...>

        Trigger the query for execution every 5 seconds

        >>> df.writeStream.trigger(continuous='5 seconds')
        <...streaming.readwriter.DataStreamWriter object ...>

        Trigger the query for reading all available data with multiple batches

        >>> df.writeStream.trigger(availableNow=True)
        <...streaming.readwriter.DataStreamWriter object ...>
        """
        params = [processingTime, once, continuous, availableNow]

        if params.count(None) == 4:
            raise PySparkValueError(
                error_class="ONLY_ALLOW_SINGLE_TRIGGER",
                message_parameters={},
            )
        elif params.count(None) < 3:
            raise PySparkValueError(
                error_class="ONLY_ALLOW_SINGLE_TRIGGER",
                message_parameters={},
            )

        processing_time = processingTime
        if processingTime is not None:
            if type(processingTime) != str or len(processingTime.strip()) == 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_NON_EMPTY_STR",
                    message_parameters={
                        "arg_name": "processingTime",
                        "arg_value": str(processingTime),
                    },
                )
            processing_time = processingTime.strip()
        elif once is not None:
            if once is not True:
                raise PySparkValueError(
                    error_class="VALUE_NOT_TRUE",
                    message_parameters={"arg_name": "once", "arg_value": str(once)},
                )
        elif continuous is not None:
            if type(continuous) != str or len(continuous.strip()) == 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_NON_EMPTY_STR",
                    message_parameters={"arg_name": "continuous", "arg_value": str(continuous)},
                )
            raise NotImplementedError()
        else:
            if availableNow is not True:
                raise PySparkValueError(
                    error_class="VALUE_NOT_TRUE",
                    message_parameters={"arg_name": "availableNow", "arg_value": str(availableNow)},
                )

        self._trigger = Trigger(
            processingTime=processing_time,
            once=once,
            availableNow=availableNow,
        )
        return self

    @staticmethod
    def _construct_foreach_function(
        f: Union[Callable[[Row], None], "SupportsProcess"]
    ) -> Callable[[Any, Iterator], Iterator]:
        from polarspark.taskcontext import TaskContext

        if callable(f):
            # The provided object is a callable function that is supposed to be called on each row.
            # Construct a function that takes an iterator and calls the provided function on each
            # row.
            def func_without_process(_: Any, iterator: Iterator) -> Iterator:
                for x in iterator:
                    f(x)  # type: ignore[operator]
                return iter([])

            return func_without_process

        else:
            # The provided object is not a callable function. Then it is expected to have a
            # 'process(row)' method, and optional 'open(partition_id, epoch_id)' and
            # 'close(error)' methods.

            if not hasattr(f, "process"):
                raise PySparkAttributeError(
                    error_class="ATTRIBUTE_NOT_CALLABLE",
                    message_parameters={"attr_name": "process", "obj_name": "f"},
                )

            if not callable(getattr(f, "process")):
                raise PySparkAttributeError(
                    error_class="ATTRIBUTE_NOT_CALLABLE",
                    message_parameters={"attr_name": "process", "obj_name": "f"},
                )

            def doesMethodExist(method_name: str) -> bool:
                exists = hasattr(f, method_name)
                if exists and not callable(getattr(f, method_name)):
                    raise PySparkAttributeError(
                        error_class="ATTRIBUTE_NOT_CALLABLE",
                        message_parameters={"attr_name": method_name, "obj_name": "f"},
                    )
                return exists

            open_exists = doesMethodExist("open")
            close_exists = doesMethodExist("close")

            def func_with_open_process_close(partition_id: Any, iterator: Iterator) -> Iterator:
                epoch_id = cast(TaskContext, TaskContext.get()).getLocalProperty(
                    "streaming.sql.batchId"
                )
                if epoch_id:
                    int_epoch_id = int(epoch_id)
                else:
                    raise PySparkRuntimeError(
                        error_class="CANNOT_GET_BATCH_ID",
                        message_parameters={"obj_name": "TaskContext"},
                    )

                # Check if the data should be processed
                should_process = True
                if open_exists:
                    should_process = f.open(partition_id, int_epoch_id)  # type: ignore[union-attr]

                error = None

                try:
                    if should_process:
                        for x in iterator:
                            cast("SupportsProcess", f).process(x)
                except Exception as ex:
                    error = ex
                finally:
                    if close_exists:
                        f.close(error)  # type: ignore[union-attr]
                    if error:
                        raise error

                return iter([])

            return func_with_open_process_close

    @overload
    def foreach(self, f: Callable[[Row], None]) -> "DataStreamWriter":
        ...

    @overload
    def foreach(self, f: "SupportsProcess") -> "DataStreamWriter":
        ...

    def foreach(self, f: Union[Callable[[Row], None], "SupportsProcess"]) -> "DataStreamWriter":
        """
        Sets the output of the streaming query to be processed using the provided writer ``f``.
        This is often used to write the output of a streaming query to arbitrary storage systems.
        The processing logic can be specified in two ways.

        #. A **function** that takes a row as input.
            This is a simple way to express your processing logic. Note that this does
            not allow you to deduplicate generated data when failures cause reprocessing of
            some input data. That would require you to specify the processing logic in the next
            way.

        #. An **object** with a ``process`` method and optional ``open`` and ``close`` methods.
            The object can have the following methods.

            * ``open(partition_id, epoch_id)``: *Optional* method that initializes the processing
                (for example, open a connection, start a transaction, etc). Additionally, you can
                use the `partition_id` and `epoch_id` to deduplicate regenerated data
                (discussed later).

            * ``process(row)``: *Non-optional* method that processes each :class:`Row`.

            * ``close(error)``: *Optional* method that finalizes and cleans up (for example,
                close connection, commit transaction, etc.) after all rows have been processed.

            The object will be used by Spark in the following way.

            * A single copy of this object is responsible of all the data generated by a
                single task in a query. In other words, one instance is responsible for
                processing one partition of the data generated in a distributed manner.

            * This object must be serializable because each task will get a fresh
                serialized-deserialized copy of the provided object. Hence, it is strongly
                recommended that any initialization for writing data (e.g. opening a
                connection or starting a transaction) is done after the `open(...)`
                method has been called, which signifies that the task is ready to generate data.

            * The lifecycle of the methods are as follows.

                For each partition with ``partition_id``:

                ... For each batch/epoch of streaming data with ``epoch_id``:

                ....... Method ``open(partitionId, epochId)`` is called.

                ....... If ``open(...)`` returns true, for each row in the partition and
                        batch/epoch, method ``process(row)`` is called.

                ....... Method ``close(errorOrNull)`` is called with error (if any) seen while
                        processing rows.

            Important points to note:

            * The `partitionId` and `epochId` can be used to deduplicate generated data when
                failures cause reprocessing of some input data. This depends on the execution
                mode of the query. If the streaming query is being executed in the micro-batch
                mode, then every partition represented by a unique tuple (partition_id, epoch_id)
                is guaranteed to have the same data. Hence, (partition_id, epoch_id) can be used
                to deduplicate and/or transactionally commit data and achieve exactly-once
                guarantees. However, if the streaming query is being executed in the continuous
                mode, then this guarantee does not hold and therefore should not be used for
                deduplication.

            * The ``close()`` method (if exists) will be called if `open()` method exists and
                returns successfully (irrespective of the return value), except if the Python
                crashes in the middle.

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> import time
        >>> df = spark.readStream.format("rate").load()

        Print every row using a function

        >>> def print_row(row):
        ...     print(row)
        ...
        >>> q = df.writeStream.foreach(print_row).start()
        >>> time.sleep(3)
        >>> q.stop()

        Print every row using a object with process() method

        >>> class RowPrinter:
        ...     def open(self, partition_id, epoch_id):
        ...         print("Opened %d, %d" % (partition_id, epoch_id))
        ...         return True
        ...
        ...     def process(self, row):
        ...         print(row)
        ...
        ...     def close(self, error):
        ...         print("Closed with error: %s" % str(error))
        ...
        >>> q = df.writeStream.foreach(print_row).start()
        >>> time.sleep(3)
        >>> q.stop()
        """

        raise NotImplementedError()

    def foreachBatch(self, func: Callable[["DataFrame", int], None]) -> "DataStreamWriter":
        """
        Sets the output of the streaming query to be processed using the provided
        function. This is supported only the in the micro-batch execution modes (that is, when the
        trigger is not continuous). In every micro-batch, the provided function will be called in
        every micro-batch with (i) the output rows as a DataFrame and (ii) the batch identifier.
        The batchId can be used deduplicate and transactionally write the output
        (that is, the provided Dataset) to external systems. The output DataFrame is guaranteed
        to exactly same for the same batchId (assuming all operations are deterministic in the
        query).

        .. versionadded:: 2.4.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        This API is evolving.
        This function behaves differently in Spark Connect mode. See examples.
        In Connect, the provided function doesn't have access to variables defined outside of it.

        Examples
        --------
        >>> import time
        >>> df = spark.readStream.format("rate").load()
        >>> my_value = -1
        >>> def func(batch_df, batch_id):
        ...     global my_value
        ...     my_value = 100
        ...     batch_df.collect()
        ...
        >>> q = df.writeStream.foreachBatch(func).start()
        >>> time.sleep(3)
        >>> q.stop()
        >>> # if in Spark Connect, my_value = -1, else my_value = 100
        """

        self._foreach_func = func
        return self

    def _get_state(self, row_num: int):
        return {
            "operatorName": "stateOperator1",
            "numRowsTotal": row_num,
            "numRowsUpdated": 0,
            "numRowsRemoved": 0,
            "allUpdatesTimeMs": 0,
            "allRemovalsTimeMs": 0,
            "commitTimeMs": 0,
            "memoryUsedBytes": 0,
            "numRowsDroppedByWatermark": 0,
            "numShufflePartitions": 0,
            "numStateStoreInstances": 0,
            "customMetrics": {"numExpiredStateRows": 0, "numTotalStateRows": 0},
        }

    def _get_source(self):
        return {
            "description": "",
            "startOffset": 0,
            "endOffset": 0,
            "latestOffset": 0,
            "numInputRows": 1,
            "inputRowsPerSecond": 0,
            "processedRowsPerSecond": 0,
            "metrics": {"avgBatchLatency": 0, "maxOffsetLag": 0},
        }

    def _get_sink(self, row_num: int):
        return {
            "description": "",
            "numOutputRows": row_num,
        }

    def start(
        self,
        path: Optional[str] = None,
        format: Optional[str] = None,
        outputMode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        queryName: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> StreamingQuery:
        """Streams the contents of the :class:`DataFrame` to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        path : str, optional
            the path in a Hadoop supported file system
        format : str, optional
            the format used to save
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.

            * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to the
              sink
            * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the
              sink every time these are some updates
            * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
              written to the sink every time there are some updates. If the query doesn't contain
              aggregations, it will be equivalent to `append` mode.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`
            for most streams, however it is not required for a `memory` stream.

        Notes
        -----
        This API is evolving.

        Examples
        --------
        >>> df = spark.readStream.format("rate").load()

        Basic example.

        >>> q = df.writeStream.format('memory').queryName('this_query').start()
        >>> q.isActive
        True
        >>> q.name
        'this_query'
        >>> q.stop()
        >>> q.isActive
        False

        Example with using other parameters with a trigger.

        >>> q = df.writeStream.trigger(processingTime='5 seconds').start(
        ...     queryName='that_query', outputMode="append", format='memory')
        >>> q.name
        'that_query'
        >>> q.isActive
        True
        >>> q.stop()
        """
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)

        self._active.set()
        run_id = str(uuid.uuid4())
        progress = []

        if self._format not in ["memory", "noop"] and not self._foreach_func:
            chk = self._options.get("checkpointLocation")
            if not chk:
                raise AnalysisException(
                    """
                checkpointLocation must be specified either through option("checkpointLocation", ...) 
                or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...)
                """
                )
            Path(chk).mkdir(parents=True, exist_ok=True)

        def starter():
            for i, ldf in enumerate(self._df._gather()):  # noqa
                if self._stream_alive(ldf):
                    if isinstance(ldf, pl.LazyFrame):
                        t = time.process_time()
                        rows = []
                        if self._foreach_func:
                            from polarspark.sql.dataframe import DataFrame

                            def ldf_generator() -> Generator[pl.LazyFrame, None, None]:
                                yield ldf

                            df = DataFrame(None, ldf_generator, self._spark)
                            self._foreach_func(df, i)
                        elif self._format == "memory":
                            if self._query_name:
                                self._spark.catalog._cat.create_or_append_in_mem_table(  # noqa
                                    self._query_name, ldf
                                )  # noqa
                        elif self._format == "noop":
                            rows = ldf.collect()
                            if i > 1:  # Seems that noop stops after first round, so stop
                                break
                        else:
                            _save(ldf=ldf, path=path, format=self._format, options=self._options)
                        duration = time.process_time() - t
                        progress.append(
                            {
                                "name": self._query_name,
                                "id": self._query_id,
                                "timestamp": str(datetime.now()),
                                "batchId": i,
                                "batchDuration": duration,
                                "runId": run_id,
                                "stateOperators": [self._get_state(len(rows))],
                                "sources": [self._get_source()],
                                "sink": self._get_sink(len(rows)),
                            }
                        )
                else:
                    break

            self._active.clear()
            self._spark.streams._remove(self._query_id)  # noqa

        self._future = self._executor.submit(starter)
        query = StreamingQuery(self, progress)
        self._spark.streams._add(query)  # noqa
        return query

    def _stream_alive(self, ldf) -> bool:
        # Apparently when it's rate it should also stop
        is_rate_format = self._df._origin and self._df._origin._format == "rate"  # noqa  # noqa

        # Check fist for explicit call to stop
        process_all = self._process_all_available.isSet()
        not_active = not self._active.isSet()

        needs_breaking = (process_all and (ldf is NO_INPUT or is_rate_format)) or not_active

        return not needs_breaking

    def toTable(
        self,
        tableName: str,
        format: Optional[str] = None,
        outputMode: Optional[str] = None,
        partitionBy: Optional[Union[str, List[str]]] = None,
        queryName: Optional[str] = None,
        **options: "OptionalPrimitiveType",
    ) -> StreamingQuery:
        """
        Starts the execution of the streaming query, which will continually output results to the
        given table as new data arrives.

        The returned :class:`StreamingQuery` object can be used to interact with the stream.

        .. versionadded:: 3.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        tableName : str
            string, for the name of the table.
        format : str, optional
            the format used to save.
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.

            * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to the
              sink
            * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the
              sink every time these are some updates
            * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
              written to the sink every time there are some updates. If the query doesn't contain
              aggregations, it will be equivalent to `append` mode.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`.

        Notes
        -----
        This API is evolving.

        For v1 table, partitioning columns provided by `partitionBy` will be respected no matter
        the table exists or not. A new table will be created if the table not exists.

        For v2 table, `partitionBy` will be ignored if the table already exists. `partitionBy` will
        be respected only if the v2 table does not exist. Besides, the v2 table created by this API
        lacks some functionalities (e.g., customized properties, options, and serde info). If you
        need them, please create the v2 table manually before the execution to avoid creating a
        table with incomplete information.

        Examples
        --------
        Save a data stream to a table.

        >>> import tempfile
        >>> import time
        >>> _ = spark.sql("DROP TABLE IF EXISTS my_table2")
        >>> with tempfile.TemporaryDirectory() as d:
        ...     # Create a table with Rate source.
        ...     q = spark.readStream.format("rate").option(
        ...         "rowsPerSecond", 10).load().writeStream.toTable(
        ...             "my_table2",
        ...             queryName='that_query',
        ...             outputMode="append",
        ...             format='parquet',
        ...             checkpointLocation=d)
        ...     time.sleep(3)
        ...     q.stop()
        ...     spark.read.table("my_table2").show()
        ...     _ = spark.sql("DROP TABLE my_table2")
        +...---------+-----+
        |...timestamp|value|
        +...---------+-----+
        ...
        """
        self.options(**options)
        if outputMode is not None:
            if outputMode != "append":
                raise NotImplementedError(f"outputMode={outputMode} is not supported yet")
            self.outputMode(outputMode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)

        if not (tbl := self._spark.catalog._cat.get_table(tableName)):  # noqa
            self._spark.catalog.createTable(tableName, schema=self._df.schema)
            tbl = self._spark.catalog._cat.get_table(tableName)  # noqa

        return self.start(path=tbl.location)


def _test() -> None:
    import doctest
    import os
    from polarspark.sql import SparkSession
    import polarspark.sql.streaming.readwriter

    os.chdir(os.environ["SPARK_HOME"])

    globs = polarspark.sql.streaming.readwriter.__dict__.copy()
    globs["spark"] = (
        SparkSession.builder.master("local[4]")
        .appName("sql.streaming.readwriter tests")
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        polarspark.sql.streaming.readwriter,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
