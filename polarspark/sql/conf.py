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
from typing import Any, Optional, Union, Mapping, Dict

from polarspark import since, _NoValue
from polarspark._globals import _NoValueType
from polarspark.errors import IllegalArgumentException
from polarspark.utils.spark_file_utils import resolve_uri


class RuntimeConfig:
    """User-facing configuration API, accessible through `SparkSession.conf`.

    Options set here are automatically propagated to the Hadoop configuration during I/O.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    _defaults = {
        "spark.sql.sources.partitionOverwriteMode": "STATIC",
        "spark.sql.crossJoin.enabled": True,
        "spark.sql.sources.default": "parquet",
    }

    _non_mutable = {"spark.sql.warehouse.dir": resolve_uri("spark-warehouse")}

    _conf: Dict

    def __init__(self, conf: dict) -> None:
        """Create a new RuntimeConfig that wraps the underlying JVM object."""
        self._conf = conf

    @since(2.0)
    def set(self, key: str, value: Union[str, int, bool]) -> None:
        """Sets the given Spark runtime configuration property."""
        if value is None:
            raise IllegalArgumentException("value can not be None")
        elif isinstance(value, (bool, int)):
            value = str(value).lower()
        if isinstance(value, str):
            self._conf[key] = value
        else:
            raise ValueError(f"Value type: {type(value)} is not supported")

    @since(2.0)
    def get(
        self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue
    ) -> Optional[str]:
        """Returns the value of Spark runtime configuration property for the given key,
        assuming it is set.
        """
        self._checkType(key, "key")

        if default is _NoValue:
            if not key in self._conf:
                if key in self._defaults:
                    return self._defaults.get(key)
                else:
                    raise KeyError(f"{key} is not set")
            return self._conf.get(key)
        else:
            if default is not None:
                self._checkType(default, "default")
            return self._conf.get(key, default)

    @since(2.0)
    def unset(self, key: str) -> None:
        """Resets the configuration property for the given key."""
        # FIX
        del self._conf[key]

    def _checkType(self, obj: Any, identifier: str) -> None:
        """Assert that an object is of type str."""
        if not isinstance(obj, str):
            raise TypeError(
                "expected %s '%s' to be a string (was '%s')" % (identifier, obj, type(obj).__name__)
            )

    @since(2.4)
    def isModifiable(self, key: str) -> bool:
        """Indicates whether the configuration property with the given key
        is modifiable in the current session.
        """
        return key not in self._non_mutable


def _test() -> None:
    import os
    import doctest
    from polarspark.sql.session import SparkSession
    import polarspark.sql.conf

    os.chdir(os.environ["SPARK_HOME"])

    globs = polarspark.sql.conf.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("sql.conf tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(polarspark.sql.conf, globs=globs)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
