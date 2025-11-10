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
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, cast


from polarspark import SparkContext
from polarspark.errors.exceptions.base import (
    AnalysisException as BaseAnalysisException,
    IllegalArgumentException as BaseIllegalArgumentException,
    ArithmeticException as BaseArithmeticException,
    UnsupportedOperationException as BaseUnsupportedOperationException,
    ArrayIndexOutOfBoundsException as BaseArrayIndexOutOfBoundsException,
    DateTimeException as BaseDateTimeException,
    NumberFormatException as BaseNumberFormatException,
    ParseException as BaseParseException,
    PySparkException,
    PythonException as BasePythonException,
    QueryExecutionException as BaseQueryExecutionException,
    SparkRuntimeException as BaseSparkRuntimeException,
    SparkUpgradeException as BaseSparkUpgradeException,
    StreamingQueryException as BaseStreamingQueryException,
    UnknownException as BaseUnknownException,
)


class CapturedException(PySparkException):
    def __init__(
        self,
        desc: Optional[str] = None,
        stackTrace: Optional[str] = None,
        cause: Optional[str] = None,
        origin: Optional[str] = None,
    ):
        # desc & stackTrace vs origin are mutually exclusive.
        # cause is optional.
        assert (origin is not None and desc is None and stackTrace is None) or (
            origin is None and desc is not None and stackTrace is not None
        )

        self.desc = desc
        self.cause = cause
        self._origin = origin

    def __str__(self) -> str:
        return self.desc

    def getErrorClass(self) -> Optional[str]:
        return None

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        return None

    def getSqlState(self) -> Optional[str]:  # type: ignore[override]
        return None


class AnalysisException(CapturedException, BaseAnalysisException):
    """
    Failed to analyze a SQL query plan.
    """


class ParseException(AnalysisException, BaseParseException):
    """
    Failed to parse a SQL command.
    """


class IllegalArgumentException(CapturedException, BaseIllegalArgumentException):
    """
    Passed an illegal or inappropriate argument.
    """


class StreamingQueryException(CapturedException, BaseStreamingQueryException):
    """
    Exception that stopped a :class:`StreamingQuery`.
    """


class QueryExecutionException(CapturedException, BaseQueryExecutionException):
    """
    Failed to execute a query.
    """


class PythonException(CapturedException, BasePythonException):
    """
    Exceptions thrown from Python workers.
    """


class ArithmeticException(CapturedException, BaseArithmeticException):
    """
    Arithmetic exception.
    """


class UnsupportedOperationException(CapturedException, BaseUnsupportedOperationException):
    """
    Unsupported operation exception.
    """


class ArrayIndexOutOfBoundsException(CapturedException, BaseArrayIndexOutOfBoundsException):
    """
    Array index out of bounds exception.
    """


class DateTimeException(CapturedException, BaseDateTimeException):
    """
    Datetime exception.
    """


class NumberFormatException(IllegalArgumentException, BaseNumberFormatException):
    """
    Number format exception.
    """


class SparkRuntimeException(CapturedException, BaseSparkRuntimeException):
    """
    Runtime exception.
    """


class SparkUpgradeException(CapturedException, BaseSparkUpgradeException):
    """
    Exception thrown because of Spark upgrade.
    """


class UnknownException(CapturedException, BaseUnknownException):
    """
    None of the above exceptions.
    """
