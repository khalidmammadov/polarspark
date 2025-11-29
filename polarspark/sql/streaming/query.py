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

import json
import sys
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event
import traceback

from polarspark.errors import StreamingQueryException, PySparkValueError
from polarspark.errors.exceptions.captured import (
    StreamingQueryException as CapturedStreamingQueryException,
)
from polarspark.sql.streaming.listener import (
    StreamingQueryListener,
    StreamingQueryProgress,
)

__all__ = ["StreamingQuery", "StreamingQueryManager"]

if TYPE_CHECKING:
    from polarspark.sql.dataframe import DataFrame


class StreamingQuery:
    """
    A handle to a query that is executing continuously in the background as new data arrives.
    All these methods are thread-safe.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, stream_writer: "DataStreamWriter", progress: list) -> None:
        self._stream_writer = stream_writer
        self._progress = progress

    @property
    def id(self) -> str:
        """
        Returns the unique id of this query that persists across restarts from checkpoint data.
        That is, this id is generated when a query is started for the first time, and
        will be the same every time it is restarted from checkpoint data.
        There can only be one query with the same id active in a Spark cluster.
        Also see, `runId`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        str
            The unique id of query that persists across restarts from checkpoint data.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get the unique id of this query that persists across restarts from checkpoint data

        >>> sq.id
        '...'

        >>> sq.stop()
        """
        return self._stream_writer._query_id  # noqa

    @property
    def runId(self) -> str:
        """
        Returns the unique id of this query that does not persist across restarts. That is, every
        query that is started (or restarted from checkpoint) will have a different runId.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        str
            The unique id of query that does not persist across restarts.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get the unique id of this query that does not persist across restarts

        >>> sq.runId
        '...'

        >>> sq.stop()
        """
        return self._stream_writer._query_id  # noqa

    @property
    def name(self) -> str:
        """
        Returns the user-specified name of the query, or null if not specified.
        This name can be specified in the `org.apache.spark.sql.streaming.DataStreamWriter`
        as `dataframe.writeStream.queryName("query").start()`.
        This name, if set, must be unique across all active queries.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        str
            The user-specified name of the query, or null if not specified.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get the user-specified name of the query, or null if not specified.

        >>> sq.name
        'this_query'

        >>> sq.stop()
        """
        return self._stream_writer._query_name  # noqa

    @property
    def isActive(self) -> bool:
        """
        Whether this streaming query is currently active or not.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        bool
            The result whether specified streaming query is currently active or not.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.isActive
        True

        >>> sq.stop()
        """
        return self._stream_writer._active.is_set()  # noqa

    def awaitTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        """
        Waits for the termination of `this` query, either by :func:`query.stop()` or by an
        exception. If the query has terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If the query has terminated, then all subsequent calls to this method will either return
        immediately (if the query was terminated by :func:`stop()`), or throw the exception
        immediately (if the query has terminated with exception).

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        timeout : int, optional
            default ``None``. The waiting time for specified streaming query to terminate.

        Returns
        -------
        bool, optional
            The result whether specified streaming query has terminated or not within the `timeout`
            seconds if `timeout` is set. The :class:`StreamingQueryException` will be thrown if the
            query has terminated with an exception.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('query_awaitTermination').start()

        Return whether the query has terminated or not within 5 seconds

        >>> sq.awaitTermination(5)
        False

        >>> sq.stop()
        """
        if not self._stream_writer._future:  # noqa
            return None

        ex = None
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "timeout", "arg_value": type(timeout).__name__},
                )
            try:
                self._stream_writer._future.result(timeout)  # noqa
            except TimeoutError:
                return False
            except Exception as e:
                ex = e
        else:
            try:
                self._stream_writer._future.result()  # noqa
            except Exception as e:
                ex = e

        if ex:
            if self._stream_writer._foreach_func:  # noqa
                raise CapturedStreamingQueryException(
                    "FOREACH_BATCH_USER_FUNCTION_ERROR: {}".format(str(ex)), traceback.format_exc()
                )
            else:
                raise ex

        return self._stream_writer._future.done()  # noqa

    @property
    def status(self) -> Dict[str, Any]:
        """
        Returns the current status of the query.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        dict
            The current status of the specified query.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get the current status of the query

        >>> sq.status
        {'message': '...', 'isDataAvailable': ..., 'isTriggerActive': ...}

        >>> sq.stop()
        """
        return json.loads("{}")

    @property
    def recentProgress(self) -> List[Dict[str, Any]]:
        """
        Returns an array of the most recent [[StreamingQueryProgress]] updates for this query.
        The number of progress updates retained for each stream is configured by Spark session
        configuration `spark.sql.streaming.numRecentProgressUpdates`.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        list
            List of dict which is the most recent :class:`StreamingQueryProgress` updates
            for this query.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get an array of the most recent query progress updates for this query

        >>> sq.recentProgress
        [...]

        >>> sq.stop()
        """
        return [StreamingQueryProgress.fromJson(p) for p in reversed(self._progress)]

    @property
    def lastProgress(self) -> Optional[Dict[str, Any]]:
        """
        Returns the most recent :class:`StreamingQueryProgress` update of this streaming query or
        None if there were no progress updates

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        dict, optional
            The most recent :class:`StreamingQueryProgress` update of this streaming query or
            None if there were no progress updates.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Get the most recent query progress updates for this query

        >>> sq.lastProgress
        >>> sq.stop()
        """
        try:
            return StreamingQueryProgress.fromJson(next(reversed(self._progress)))
        except StopIteration:
            return None

    def processAllAvailable(self) -> None:
        """
        Blocks until all available data in the source has been processed and committed to the
        sink. This method is intended for testing.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Notes
        -----
        In the case of continually arriving data, this method may block forever.
        Additionally, this method is only guaranteed to block until data that has been
        synchronously appended data to a stream source prior to invocation.
        (i.e. `getOffset` must immediately reflect the addition).

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Blocks query until all available data in the source
        has been processed and committed to the sink

        >>> sq.processAllAvailable
        <bound method StreamingQuery.processAllAvailable ...>

        >>> sq.stop()
        """
        self._stream_writer._process_all_available.set()  # noqa
        return self.awaitTermination()

    def stop(self) -> None:
        """
        Stop this streaming query.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.isActive
        True

        Stop streaming query

        >>> sq.stop()

        >>> sq.isActive
        False
        """
        self._stream_writer._active.clear()  # noqa
        self._stream_writer._future.cancel()  # noqa
        self._stream_writer._spark.streams._remove(self.id)  # noqa

    def explain(self, extended: bool = False) -> None:
        """
        Prints the (logical and physical) plans to the console for debugging purpose.

        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        extended : bool, optional
            default ``False``. If ``False``, prints only the physical plan.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sdf.printSchema()
        root
          |-- timestamp: timestamp (nullable = true)
          |-- value: long (nullable = true)

        >>> sq = sdf.writeStream.format('memory').queryName('query_explain').start()
        >>> sq.processAllAvailable() # Wait a bit to generate the runtime plans.

        Explain the runtime plans

        >>> sq.explain()
        == Physical Plan ==
        ...
        >>> sq.explain(True)
        == Parsed Logical Plan ==
        ...
        == Analyzed Logical Plan ==
        ...
        == Optimized Logical Plan ==
        ...
        == Physical Plan ==
        ...
        >>> sq.stop()
        """
        self._stream_writer._df.explain(extended)  # noqa

    def exception(self) -> Optional[StreamingQueryException]:
        """
        .. versionadded:: 2.1.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        :class:`StreamingQueryException`
            the StreamingQueryException if the query was terminated by an exception, or None.
        """
        try:
            ex = self._stream_writer._future.exception(0.1)  # noqa
        except TimeoutError as _:
            return None
        except Exception as e:
            ex = e

        if ex:
            return CapturedStreamingQueryException(
                "FOREACH_BATCH_USER_FUNCTION_ERROR: {}".format(str(ex)), traceback.format_exc()
            )
        return None


class StreamingQueryManager:
    """A class to manage all the :class:`StreamingQuery` StreamingQueries active.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Notes
    -----
    This API is evolving.
    """

    def __init__(self) -> None:
        self._queries: Dict[str, StreamingQuery] = {}
        self._terminated: List[StreamingQuery] = []

    @property
    def active(self) -> List[StreamingQuery]:
        """
        Returns a list of active queries associated with this SparkSession

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Returns
        -------
        list
            The active queries associated with this :class:`SparkSession`.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sdf.printSchema()
        root
          |-- timestamp: timestamp (nullable = true)
          |-- value: long (nullable = true)

        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sqm = spark.streams

        Get the list of active streaming queries

        >>> [q.name for q in sqm.active]
        ['this_query']
        >>> sq.stop()
        """
        return [q for q in self._queries.values() if q.isActive]

    def _add(self, q: StreamingQuery):
        self._queries[q.id] = q

    def _remove(self, _id: str):
        if res := self._queries.pop(_id, None):
            self._terminated.append(res)

    def get(self, id: str) -> Optional[StreamingQuery]:
        """
        Returns an active query from this :class:`SparkSession`.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        id : str
            The unique id of specified query.

        Returns
        -------
        :class:`StreamingQuery`
            An active query with `id` from this SparkSession.

        Notes
        -----
        Exception will be thrown if an active query with this id does not exist.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sdf.printSchema()
        root
          |-- timestamp: timestamp (nullable = true)
          |-- value: long (nullable = true)

        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()
        >>> sq.name
        'this_query'

        Get an active query by id

        >>> sq = spark.streams.get(sq.id)
        >>> sq.isActive
        True
        >>> sq.stop()
        """
        return self._queries.get(id)

    def awaitAnyTermination(self, timeout: Optional[int] = None) -> Optional[bool]:
        """
        Wait until any of the queries on the associated SparkSession has terminated since the
        creation of the context, or since :func:`resetTerminated()` was called. If any query was
        terminated with an exception, then the exception will be thrown.
        If `timeout` is set, it returns whether the query has terminated or not within the
        `timeout` seconds.

        If a query has terminated, then subsequent calls to :func:`awaitAnyTermination()` will
        either return immediately (if the query was terminated by :func:`query.stop()`),
        or throw the exception immediately (if the query was terminated with exception). Use
        :func:`resetTerminated()` to clear past terminations and wait for new terminations.

        In the case where multiple queries have terminated since :func:`resetTermination()`
        was called, if any query has terminated with exception, then :func:`awaitAnyTermination()`
        will throw any of the exception. For correctly documenting exceptions across multiple
        queries, users need to stop all of them after any of them terminates with exception, and
        then check the `query.exception()` for each query.

        throws :class:`StreamingQueryException`, if `this` query has terminated with an exception

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Parameters
        ----------
        timeout : int, optional
            default ``None``. The waiting time for any streaming query to terminate.

        Returns
        -------
        bool, optional
            The result whether any streaming query has terminated or not within the `timeout`
            seconds if `timeout` is set. The :class:`StreamingQueryException` will be thrown if any
            query has terminated with an exception.

        Examples
        --------
        >>> sdf = spark.readStream.format("rate").load()
        >>> sq = sdf.writeStream.format('memory').queryName('this_query').start()

        Return whether any of the query on the associated SparkSession
        has terminated or not within 5 seconds

        >>> spark.streams.awaitAnyTermination(5)
        True
        >>> sq.stop()
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout < 0:
                raise PySparkValueError(
                    error_class="VALUE_NOT_POSITIVE",
                    message_parameters={"arg_name": "timeout", "arg_value": type(timeout).__name__},
                )

            # The goal is to poll periodically
            # as query can terminate in any second within timeout
            timeouted = Event()

            def check() -> bool:
                while not timeouted.is_set():
                    if self._terminated:
                        return True
                return False

            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(check)
                try:
                    fut.result(timeout=int(timeout or sys.maxsize))
                except TimeoutError:
                    timeouted.set()
                    return False

        # Exception checking phase
        for q in self._terminated:
            if q.exception():
                raise q.exception()

        # This method is overloaded in Scala
        # one returns Boolean and Unit other one
        if timeout:
            return True
        # After waiting indeterminately and
        # seeing completion no errors were observed
        return None

    def resetTerminated(self) -> None:
        """
        Forget about past terminated queries so that :func:`awaitAnyTermination()` can be used
        again to wait for new terminations.

        .. versionadded:: 2.0.0

        .. versionchanged:: 3.5.0
            Supports Spark Connect.

        Examples
        --------
        >>> spark.streams.resetTerminated()
        """
        self._terminated = []

    # def addListener(self, listener: StreamingQueryListener) -> None:
    #     """
    #     Register a :class:`StreamingQueryListener` to receive up-calls for life cycle events of
    #     :class:`~pyspark.sql.streaming.StreamingQuery`.
    #
    #     .. versionadded:: 3.4.0
    #
    #     .. versionchanged:: 3.5.0
    #         Supports Spark Connect.
    #
    #     Parameters
    #     ----------
    #     listener : :class:`StreamingQueryListener`
    #         A :class:`StreamingQueryListener` to receive up-calls for life cycle events of
    #         :class:`~pyspark.sql.streaming.StreamingQuery`.
    #
    #     Notes
    #     -----
    #     This function behaves differently in Spark Connect mode.
    #     In Connect, the provided functions doesn't have access to variables defined outside of it.
    #     Also in Connect, you need to use `self.spark` to access spark session.
    #     Using `spark` would throw an exception.
    #     In short, if you want to use spark session inside the listener,
    #     please use `self.spark` in Connect mode, and use `spark` otherwise.
    #
    #     Examples
    #     --------
    #     >>> from pyspark.sql.streaming import StreamingQueryListener
    #     >>> class TestListener(StreamingQueryListener):
    #     ...     def onQueryStarted(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryProgress(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryIdle(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryTerminated(self, event):
    #     ...         pass
    #     ...
    #     >>> test_listener = TestListener()
    #
    #     Register streaming query listener
    #
    #     >>> spark.streams.addListener(test_listener)
    #
    #     Deregister streaming query listener
    #
    #     >>> spark.streams.removeListener(test_listener)
    #     """
    #     from polarspark import SparkContext
    #     from pyspark.java_gateway import ensure_callback_server_started
    #
    #     gw = SparkContext._gateway
    #     assert gw is not None
    #     java_import(gw.jvm, "org.apache.spark.sql.streaming.*")
    #     ensure_callback_server_started(gw)
    #
    #     self._jsqm.addListener(listener._jlistener)
    #
    # def removeListener(self, listener: StreamingQueryListener) -> None:
    #     """
    #     Deregister a :class:`StreamingQueryListener`.
    #
    #     .. versionadded:: 3.4.0
    #
    #     Parameters
    #     ----------
    #     listener : :class:`StreamingQueryListener`
    #         A :class:`StreamingQueryListener` to receive up-calls for life cycle events of
    #         :class:`~pyspark.sql.streaming.StreamingQuery`.
    #
    #     Examples
    #     --------
    #     >>> from pyspark.sql.streaming import StreamingQueryListener
    #     >>> class TestListener(StreamingQueryListener):
    #     ...     def onQueryStarted(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryProgress(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryIdle(self, event):
    #     ...         pass
    #     ...
    #     ...     def onQueryTerminated(self, event):
    #     ...         pass
    #     ...
    #     >>> test_listener = TestListener()
    #
    #     Register streaming query listener
    #
    #     >>> spark.streams.addListener(test_listener)
    #
    #     Deregister streaming query listener
    #
    #     >>> spark.streams.removeListener(test_listener)
    #     """
    #     self._jsqm.removeListener(listener._jlistener)


def _test() -> None:
    import doctest
    import os
    import sys
    from polarspark.sql import SparkSession
    import polarspark.sql.streaming.query
    from py4j.protocol import Py4JError

    os.chdir(os.environ["SPARK_HOME"])

    globs = polarspark.sql.streaming.query.__dict__.copy()
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except Py4JError:  # noqa: F821
        spark = SparkSession(sc)  # type: ignore[name-defined] # noqa: F821

    globs["spark"] = spark

    (failure_count, test_count) = doctest.testmod(
        polarspark.sql.streaming.query,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
