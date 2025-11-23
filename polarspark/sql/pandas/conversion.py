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
import sys
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
    no_type_check,
    overload,
    TYPE_CHECKING,
)
from warnings import warn
import tzlocal

# from polarspark.loose_version import LooseVersion
# from polarspark.rdd import _load_from_socket
from polarspark.sql.pandas.serializers import ArrowCollectSerializer
from polarspark.sql.pandas.types import _dedup_names
from polarspark.sql.types import (
    ArrayType,
    MapType,
    TimestampType,
    StructType,
    DataType,
    _create_row,
)
from polarspark.sql.utils import is_timestamp_ntz_preferred
from polarspark.traceback_utils import SCCallSiteSync
from polarspark.errors import PySparkTypeError

import polars as pl

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa
    from py4j.java_gateway import JavaObject

    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike
    from pyspark.sql import DataFrame


class PandasConversionMixin:
    """
    Mix-in for the conversion from Spark to pandas. Currently, only :class:`DataFrame`
    can use this class.
    """

    def toPandas(self) -> "PandasDataFrameLike":
        """
        Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This is only available if Pandas is installed and available.

        .. versionadded:: 1.3.0

        .. versionchanged:: 3.4.0
            Supports Spark Connect.

        Notes
        -----
        This method should only be used if the resulting Pandas ``pandas.DataFrame`` is
        expected to be small, as all the data is loaded into the driver's memory.

        Usage with ``spark.sql.execution.arrow.pyspark.enabled=True`` is experimental.

        Examples
        --------
        >>> df.toPandas()  # doctest: +SKIP
           age   name
        0    2  Alice
        1    5    Bob
        """
        from polarspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        from polarspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()
        return self._gather_first().collect().to_pandas(use_pyarrow_extension_array=True)

    def _collect_as_arrow(self, split_batches: bool = False) -> List["pa.RecordBatch"]:
        """
        Returns all records as a list of ArrowRecordBatches, pyarrow must be installed
        and available on driver and worker Python environments.
        This is an experimental feature.

        :param split_batches: split batches such that each column is in its own allocation, so
            that the selfDestruct optimization is effective; default False.

        .. note:: Experimental.
        """
        from pyspark.sql.dataframe import DataFrame

        assert isinstance(self, DataFrame)

        with SCCallSiteSync(self._sc):
            (
                port,
                auth_secret,
                jsocket_auth_server,
            ) = self._jdf.collectAsArrowToPython()

        # Collect list of un-ordered batches where last element is a list of correct order indices
        try:
            batch_stream = _load_from_socket((port, auth_secret), ArrowCollectSerializer())
            if split_batches:
                # When spark.sql.execution.arrow.pyspark.selfDestruct.enabled, ensure
                # each column in each record batch is contained in its own allocation.
                # Otherwise, selfDestruct does nothing; it frees each column as its
                # converted, but each column will actually be a list of slices of record
                # batches, and so no memory is actually freed until all columns are
                # converted.
                import pyarrow as pa

                results = []
                for batch_or_indices in batch_stream:
                    if isinstance(batch_or_indices, pa.RecordBatch):
                        batch_or_indices = pa.RecordBatch.from_arrays(
                            [
                                # This call actually reallocates the array
                                pa.concat_arrays([array])
                                for array in batch_or_indices
                            ],
                            schema=batch_or_indices.schema,
                        )
                    results.append(batch_or_indices)
            else:
                results = list(batch_stream)
        finally:
            pass

        # Separate RecordBatches from batch order indices in results
        batches = results[:-1]
        batch_order = results[-1]

        # Re-order the batch list using the correct order
        return [batches[i] for i in batch_order]


def schema_from_pandas(
    pdf: "PandasDataFrameLike", schema: Optional[Union[StructType, List[str]]] = None
):
    import pyarrow as pa
    from polarspark.sql.pandas.types import from_arrow_type

    if schema is None:
        schema = pdf.columns
    if isinstance(schema, (list, tuple)):
        arrow_schema = pa.Schema.from_pandas(pdf, preserve_index=False)
        struct = StructType()
        prefer_timestamp_ntz = is_timestamp_ntz_preferred()
        for name, field in zip(schema, arrow_schema):
            struct.add(
                name, from_arrow_type(field.type, prefer_timestamp_ntz), nullable=field.nullable
            )
        schema = struct
    return schema


def schema_from_polars(pdf: pl.DataFrame):
    from polarspark.sql.pandas.types import from_arrow_type

    arrow_schema = pdf.to_arrow().schema
    struct = StructType()
    prefer_timestamp_ntz = is_timestamp_ntz_preferred()
    for name, field in zip(pdf.columns, arrow_schema):
        struct.add(name, from_arrow_type(field.type, prefer_timestamp_ntz), nullable=field.nullable)
    return struct


class SparkConversionMixin:
    """
    Min-in for the conversion from pandas to Spark. Currently, only :class:`SparkSession`
    can use this class.
    """

    _jsparkSession: "JavaObject"

    @overload
    def createDataFrame(
        self, data: "PandasDataFrameLike", samplingRatio: Optional[float] = ...
    ) -> "DataFrame":
        ...

    @overload
    def createDataFrame(
        self,
        data: "PandasDataFrameLike",
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> "DataFrame":
        ...

    def createDataFrame(  # type: ignore[misc]
        self,
        data: "PandasDataFrameLike",
        schema: Optional[Union[StructType, List[str]]] = None,
        samplingRatio: Optional[float] = None,
        verifySchema: bool = True,
    ) -> "DataFrame":
        from polarspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        from polarspark.sql.pandas.utils import require_minimum_pandas_version

        require_minimum_pandas_version()

        return self._create_base_dataframe(pl.from_pandas(data).lazy())

    def _convert_from_pandas(
        self, pdf: "PandasDataFrameLike", schema: Union[StructType, str, List[str]], timezone: str
    ) -> List:
        """
        Convert a pandas.DataFrame to list of records that can be used to make a DataFrame

        Returns
        -------
        list
            list of records
        """
        from pyspark.sql import SparkSession

        assert isinstance(self, SparkSession)

        if timezone is not None:
            from pyspark.sql.pandas.types import (
                _check_series_convert_timestamps_tz_local,
                _get_local_timezone,
            )
            import pandas as pd
            from pandas.core.dtypes.common import is_timedelta64_dtype

            copied = False
            if isinstance(schema, StructType):

                def _create_converter(data_type: DataType) -> Callable[[pd.Series], pd.Series]:
                    if isinstance(data_type, TimestampType):

                        def correct_timestamp(pser: pd.Series) -> pd.Series:
                            return _check_series_convert_timestamps_tz_local(pser, timezone)

                        return correct_timestamp

                    def _converter(dt: DataType) -> Optional[Callable[[Any], Any]]:
                        if isinstance(dt, ArrayType):
                            element_conv = _converter(dt.elementType) or (lambda x: x)

                            def convert_array(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return [element_conv(v) for v in value]

                            return convert_array

                        elif isinstance(dt, MapType):
                            key_conv = _converter(dt.keyType) or (lambda x: x)
                            value_conv = _converter(dt.valueType) or (lambda x: x)

                            def convert_map(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return {key_conv(k): value_conv(v) for k, v in value.items()}

                            return convert_map

                        elif isinstance(dt, StructType):
                            field_names = dt.names
                            dedup_field_names = _dedup_names(field_names)
                            field_convs = [
                                _converter(f.dataType) or (lambda x: x) for f in dt.fields
                            ]

                            def convert_struct(value: Any) -> Any:
                                if value is None:
                                    return None
                                elif isinstance(value, dict):
                                    _values = [
                                        field_convs[i](value.get(name, None))
                                        for i, name in enumerate(dedup_field_names)
                                    ]
                                    return _create_row(field_names, _values)
                                else:
                                    _values = [
                                        field_convs[i](value[i]) for i, name in enumerate(value)
                                    ]
                                    return _create_row(field_names, _values)

                            return convert_struct

                        elif isinstance(dt, TimestampType):

                            def convert_timestamp(value: Any) -> Any:
                                if value is None:
                                    return None
                                else:
                                    return (
                                        pd.Timestamp(value)
                                        .tz_localize(timezone, ambiguous=False)  # type: ignore
                                        .tz_convert(_get_local_timezone())
                                        .tz_localize(None)
                                        .to_pydatetime()
                                    )

                            return convert_timestamp

                        else:
                            return None

                    conv = _converter(data_type)
                    if conv is not None:
                        return lambda pser: pser.apply(conv)  # type: ignore[return-value]
                    else:
                        return lambda pser: pser

                if len(pdf.columns) > 0:
                    pdf = pd.concat(
                        [
                            _create_converter(field.dataType)(pser)
                            for (_, pser), field in zip(pdf.items(), schema.fields)
                        ],
                        axis="columns",
                    )
                    copied = True
            else:
                should_localize = not is_timestamp_ntz_preferred()
                for column, series in pdf.items():
                    s = series
                    if (
                        should_localize
                        and isinstance(s.dtype, pd.DatetimeTZDtype)
                        and s.dt.tz is not None
                    ):
                        s = _check_series_convert_timestamps_tz_local(series, timezone)
                    if s is not series:
                        if not copied:
                            # Copy once if the series is modified to prevent the original
                            # Pandas DataFrame from being updated
                            pdf = pdf.copy()
                            copied = True
                        pdf[column] = s

            for column, series in pdf.items():
                if is_timedelta64_dtype(series):
                    if not copied:
                        pdf = pdf.copy()
                        copied = True
                    # Explicitly set the timedelta as object so the output of numpy records can
                    # hold the timedelta instances as are. Otherwise, it converts to the internal
                    # numeric values.
                    ser = pdf[column]
                    pdf[column] = pd.Series(
                        ser.dt.to_pytimedelta(), index=ser.index, dtype="object", name=ser.name
                    )

        # Convert pandas.DataFrame to list of numpy records
        np_records = pdf.set_axis(
            [f"col_{i}" for i in range(len(pdf.columns))], axis="columns"  # type: ignore[arg-type]
        ).to_records(index=False)

        # Check if any columns need to be fixed for Spark to infer properly
        if len(np_records) > 0:
            record_dtype = self._get_numpy_record_dtype(np_records[0])
            if record_dtype is not None:
                return [r.astype(record_dtype).tolist() for r in np_records]

        # Convert list of numpy records to python lists
        return [r.tolist() for r in np_records]

    def _get_numpy_record_dtype(self, rec: "np.recarray") -> Optional["np.dtype"]:
        """
        Used when converting a pandas.DataFrame to Spark using to_records(), this will correct
        the dtypes of fields in a record so they can be properly loaded into Spark.

        Parameters
        ----------
        rec : numpy.record
            a numpy record to check field dtypes

        Returns
        -------
        numpy.dtype
            corrected dtype for a numpy.record or None if no correction needed
        """
        import numpy as np

        cur_dtypes = rec.dtype
        col_names = cur_dtypes.names
        record_type_list = []
        has_rec_fix = False
        for i in range(len(cur_dtypes)):
            curr_type = cur_dtypes[i]
            # If type is a datetime64 timestamp, convert to microseconds
            # NOTE: if dtype is datetime[ns] then np.record.tolist() will output values as longs,
            # conversion from [us] or lower will lead to py datetime objects, see SPARK-22417
            if curr_type == np.dtype("datetime64[ns]"):
                curr_type = "datetime64[us]"
                has_rec_fix = True
            record_type_list.append((str(col_names[i]), curr_type))
        return np.dtype(record_type_list) if has_rec_fix else None


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.pandas.conversion

    globs = pyspark.sql.pandas.conversion.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.pandas.conversion tests").getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.pandas.conversion,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
