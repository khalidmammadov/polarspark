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
import warnings
from typing import Any, Callable, NamedTuple, List, Optional, TYPE_CHECKING, Generator
import re
import pathlib

from polarspark.sql._internal.executor.loader import load_df_from_location
from polarspark.sql._internal.parser.models import SourceRelation
from polarspark.storagelevel import StorageLevel
from polarspark.sql.dataframe import DataFrame
from polarspark.sql.session import SparkSession
from polarspark.sql.types import StructType
from polarspark.errors import AnalysisException

from polarspark.sql._internal.catalog.in_memory_catalog import InMemoryCatalog  # noqa
from polarspark.sql._internal.catalog.utils import parse_table_name, TableName  # noqa


if TYPE_CHECKING:
    from polarspark.sql._typing import UserDefinedFunctionLike
    from polarspark.sql.types import DataType
    import polars as pl


class CatalogMetadata(NamedTuple):
    name: str
    description: Optional[str]


class Database(NamedTuple):
    name: str
    catalog: Optional[str]
    description: Optional[str]
    locationUri: str


class Table(NamedTuple):
    name: str
    catalog: Optional[str]
    namespace: Optional[List[str]]
    description: Optional[str]
    tableType: str
    isTemporary: bool

    @property
    def database(self) -> Optional[str]:
        if self.namespace is not None and len(self.namespace) == 1:
            return self.namespace[0]
        else:
            return None


class Column(NamedTuple):
    name: str
    description: Optional[str]
    dataType: str
    nullable: bool
    isPartition: bool
    isBucket: bool


class Function(NamedTuple):
    name: str
    catalog: Optional[str]
    namespace: Optional[List[str]]
    description: Optional[str]
    className: str
    isTemporary: bool


class Catalog:
    """User-facing catalog API, accessible through `SparkSession.catalog`.

    This is a thin wrapper around its Scala implementation org.apache.spark.sql.catalog.Catalog.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.
    """

    DEFAULT_SPARK_PATH = "spark-warehouse"

    _default_catalog = "spark_catalog"
    _default_database = "default"
    _current_catalog = _default_catalog
    _current_database = _default_database
    _cached_tables = {}

    def __init__(self, sparkSession: SparkSession) -> None:
        """Create a new Catalog that wraps the underlying JVM object."""
        self._sparkSession = sparkSession

        self._cat = InMemoryCatalog()

    def currentCatalog(self) -> str:
        """Returns the current default catalog in this session.

        .. versionadded:: 3.4.0

        Examples
        --------
        >>> spark.catalog.currentCatalog()
        'spark_catalog'
        """
        cat_str, _ = self._cat.get_current_catalog()
        return cat_str

    def setCurrentCatalog(self, catalogName: str) -> None:
        """Sets the current default catalog in this session.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        catalogName : str
            name of the catalog to set

        Examples
        --------
        >>> spark.catalog.setCurrentCatalog("spark_catalog")
        """
        self._cat.set_current_catalog(catalogName)

    def listCatalogs(self, pattern: Optional[str] = None) -> List[CatalogMetadata]:
        """Returns a list of catalogs in this session.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        pattern : str
            The pattern that the catalog name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`CatalogMetadata`.

        Examples
        --------
        >>> spark.catalog.listCatalogs()
        [CatalogMetadata(name='spark_catalog', description=None)]

        >>> spark.catalog.listCatalogs("spark*")
        [CatalogMetadata(name='spark_catalog', description=None)]

        >>> spark.catalog.listCatalogs("hive*")
        []
        """
        if pattern is None:
            it = self._cat.catalogs().keys()
        else:
            it = [
                s for s in self._cat.catalogs().keys() if re.match(self._regex_pattern(pattern), s)
            ]
        catalogs = []
        for i in it:
            catalogs.append(CatalogMetadata(name=i, description=""))
        return catalogs

    def currentDatabase(self) -> str:
        """
        Returns the current default database in this session.

        .. versionadded:: 2.0.0

        Returns
        -------
        str
            The current default database name.

        Examples
        --------
        >>> spark.catalog.currentDatabase()
        'default'
        """
        return self._cat.get_current_database_name()

    def setCurrentDatabase(self, dbName: str) -> None:
        """
        Sets the current default database in this session.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> spark.catalog.setCurrentDatabase("default")
        """
        self._cat.set_current_database(dbName)

    def listDatabases(self, pattern: Optional[str] = None) -> List[Database]:
        """
        Returns a list of databases available across all sessions.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        pattern : str
            The pattern that the database name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Database`.

        Examples
        --------
        >>> spark.catalog.listDatabases()
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.listDatabases("def*")
        [Database(name='default', catalog='spark_catalog', description='default database', ...

        >>> spark.catalog.listDatabases("def2*")
        []
        """
        cat_name = self._cat.get_current_catalog_name()

        def it():
            for _db in self._cat.get_current_catalog():
                if pattern:
                    if re.match(pattern, _db):
                        yield db, cat_name
                else:
                    yield db, cat_name

        databases = []
        for db, cat in it():
            databases.append(
                Database(
                    name=db,
                    catalog=cat,
                    description="",
                    locationUri="",
                )
            )
        return databases

    def getDatabase(self, dbName: str) -> Database:
        """Get the database with the specified name.
        This throws an :class:`AnalysisException` when the database cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str
             name of the database to get.

        Returns
        -------
        :class:`Database`
            The database found by the name.

        Examples
        --------
        >>> spark.catalog.getDatabase("default")
        Database(name='default', catalog='spark_catalog', description='default database', ...

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.getDatabase("spark_catalog.default")
        Database(name='default', catalog='spark_catalog', description='default database', ...
        """
        cat_name = self._cat.get_current_catalog_name()
        for _db in self._cat.get_current_catalog():
            if dbName == _db:
                return Database(
                    name=dbName,
                    catalog=cat_name,
                    description="",
                    locationUri="",
                )

        raise AnalysisException(f"Database '{dbName}' not found.")

    def databaseExists(self, dbName: str) -> bool:
        """Check if the database with the specified name exists.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        dbName : str
            name of the database to check existence

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        Returns
        -------
        bool
            Indicating whether the database exists

        Examples
        --------
        Check if 'test_new_database' database exists

        >>> spark.catalog.databaseExists("test_new_database")
        False
        >>> _ = spark.sql("CREATE DATABASE test_new_database")
        >>> spark.catalog.databaseExists("test_new_database")
        True

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.databaseExists("spark_catalog.test_new_database")
        True
        >>> _ = spark.sql("DROP DATABASE test_new_database")
        """
        try:
            self.getDatabase(dbName)
        except AnalysisException:
            return False
        return True

    def listTables(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Table]:
        """Returns a list of tables/views in the specified database.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        dbName : str
            name of the database to list the tables.

            .. versionchanged:: 3.4.0
               Allow ``dbName`` to be qualified with catalog name.

        pattern : str
            The pattern that the database name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Table`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> spark.range(1).createTempView("test_view")
        >>> spark.catalog.listTables()
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.listTables(pattern="test*")
        [Table(name='test_view', catalog=None, namespace=[], description=None, ...

        >>> spark.catalog.listTables(pattern="table*")
        []

        >>> _ = spark.catalog.dropTempView("test_view")
        >>> spark.catalog.listTables()
        []
        """
        if dbName is None:
            dbName = self._cat.get_current_database_name()

        _dbs = self._cat.get_current_catalog()

        tables = []
        for t in _dbs[dbName].tables:
            tables.append(
                Table(
                    name=t,
                    catalog=self._cat.get_current_catalog_name(),
                    namespace=[self._current_database],
                    description="",
                    tableType="TEMPORARY",
                    isTemporary=True,
                )
            )
        return tables

    def getTable(self, tableName: str) -> Table:
        """Get the table or view with the specified name. This table can be a temporary view or a
        table/view. This throws an :class:`AnalysisException` when no Table can be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
               Allow `tableName` to be qualified with catalog name.

        Returns
        -------
        :class:`Table`
            The table found by the name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.getTable("tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name with the catalog name.

        >>> spark.catalog.getTable("default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getTable("spark_catalog.default.tbl1")
        Table(name='tbl1', catalog='spark_catalog', namespace=['default'], ...
        >>> _ = spark.sql("DROP TABLE tbl1")

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.getTable("tbl1")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        names = parse_table_name(tableName)
        _cat = names.catalog or self._cat.get_current_catalog_name()
        _db = names.database or self._cat.get_current_database_name()
        try:
            _dbs = self._cat.catalogs()[_cat]
            if tableName in _dbs[_db].tables:
                return Table(
                    name=names.table,
                    catalog=names.catalog,
                    namespace=[_db],
                    description="",
                    tableType="TEMPORARY",
                    isTemporary=True,
                )
        finally:
            raise AnalysisException(f"Database '{tableName}' not found.")

    def listFunctions(
        self, dbName: Optional[str] = None, pattern: Optional[str] = None
    ) -> List[Function]:
        """
        Returns a list of functions registered in the specified database.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        dbName : str
            name of the database to list the functions.
            ``dbName`` can be qualified with catalog name.
        pattern : str
            The pattern that the function name needs to match.

            .. versionadded: 3.5.0

        Returns
        -------
        list
            A list of :class:`Function`.

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.listFunctions()
        [Function(name=...

        >>> spark.catalog.listFunctions(pattern="to_*")
        [Function(name=...

        >>> spark.catalog.listFunctions(pattern="*not_existing_func*")
        []
        """
        raise NotImplementedError("Functions are not implemented")

    def functionExists(self, functionName: str, dbName: Optional[str] = None) -> bool:
        """Check if the function with the specified name exists.
        This can either be a temporary function or a function.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        functionName : str
            name of the function to check existence

            .. versionchanged:: 3.4.0
               Allow ``functionName`` to be qualified with catalog name

        dbName : str, optional
            name of the database to check function existence in.

        Returns
        -------
        bool
            Indicating whether the function exists

        Notes
        -----
        If no database is specified, the current database and catalog
        are used. This API includes all temporary functions.

        Examples
        --------
        >>> spark.catalog.functionExists("count")
        True

        Using the fully qualified name for function name.

        >>> spark.catalog.functionExists("default.unexisting_function")
        False
        >>> spark.catalog.functionExists("spark_catalog.default.unexisting_function")
        False
        """
        raise NotImplementedError("Functions are not implemented")

    def getFunction(self, functionName: str) -> Function:
        """Get the function with the specified name. This function can be a temporary function or a
        function. This throws an :class:`AnalysisException` when the function cannot be found.

        .. versionadded:: 3.4.0

        Parameters
        ----------
        functionName : str
            name of the function to check existence.

        Returns
        -------
        :class:`Function`
            The function found by the name.

        Examples
        --------
        >>> _ = spark.sql(
        ...     "CREATE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
        >>> spark.catalog.getFunction("my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Using the fully qualified name for function name.

        >>> spark.catalog.getFunction("default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...
        >>> spark.catalog.getFunction("spark_catalog.default.my_func1")
        Function(name='my_func1', catalog='spark_catalog', namespace=['default'], ...

        Throw an analysis exception when the function does not exists.

        >>> spark.catalog.getFunction("my_func2")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        raise NotImplementedError("Functions are not implemented")

    def listColumns(self, tableName: str, dbName: Optional[str] = None) -> List[Column]:
        """Returns a list of columns for the given table/view in the specified database.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to list columns.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, optional
            name of the database to find the table to list columns.

        Returns
        -------
        list
            A list of :class:`Column`.

        Notes
        -----
        The order of arguments here is different from that of its JVM counterpart
        because Python does not support method overloading.

        If no database is specified, the current database and catalog
        are used. This API includes all temporary views.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tblA (name STRING, age INT) USING parquet")
        >>> spark.catalog.listColumns("tblA")
        [Column(name='name', description=None, dataType='string', nullable=True, ...
        >>> _ = spark.sql("DROP TABLE tblA")
        """
        tbl_name = None
        if dbName is None:
            tbl_name = parse_table_name(tableName).table
        else:
            warnings.warn(
                "`dbName` has been deprecated since Spark 3.4 and might be removed in "
                "a future version. Use listColumns(`dbName.tableName`) instead.",
                FutureWarning,
            )
        tbl_name = tbl_name or tableName

        def df_generator() -> Generator[pl.LazyFrame, None, None]:
            _cat = self._cat.get_current_catalog()
            ts = _cat[dbName or self._cat.get_current_database_name()]
            yield ts.pl_ctx.execute(f"select * from {tbl_name}")  # noqa

        df = DataFrame(None, df_generator, self._sparkSession, alias=tableName)

        columns = []
        for cname, ctype in df.dtypes:
            columns.append(
                Column(
                    name=cname,
                    description="",
                    dataType=ctype,
                    nullable=True,
                    isPartition=False,
                    isBucket=False,
                )
            )
        return columns

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        """Check if the table or view with the specified name exists.
        This can either be a temporary view or a table/view.

        .. versionadded:: 3.3.0

        Parameters
        ----------
        tableName : str
            name of the table to check existence.
            If no database is specified, first try to treat ``tableName`` as a
            multi-layer-namespace identifier, then try ``tableName`` as a normal table
            name in the current database if necessary.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name when ``dbName`` is None.

        dbName : str, optional
            name of the database to check table existence in.

        Returns
        -------
        bool
            Indicating whether the table/view exists

        Examples
        --------
        This function can check if a table is defined or not:

        >>> spark.catalog.tableExists("unexisting_table")
        False
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.tableExists("tbl1")
        True

        Using the fully qualified names for tables.

        >>> spark.catalog.tableExists("default.tbl1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.tableExists("tbl1", "default")
        True
        >>> _ = spark.sql("DROP TABLE tbl1")

        Check if views exist:

        >>> spark.catalog.tableExists("view1")
        False
        >>> _ = spark.sql("CREATE VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True

        Using the fully qualified names for views.

        >>> spark.catalog.tableExists("default.view1")
        True
        >>> spark.catalog.tableExists("spark_catalog.default.view1")
        True
        >>> spark.catalog.tableExists("view1", "default")
        True
        >>> _ = spark.sql("DROP VIEW view1")

        Check if temporary views exist:

        >>> _ = spark.sql("CREATE TEMPORARY VIEW view1 AS SELECT 1")
        >>> spark.catalog.tableExists("view1")
        True
        >>> df = spark.sql("DROP VIEW view1")
        >>> spark.catalog.tableExists("view1")
        False
        """
        _cat = self._cat.get_current_catalog()
        _cdb = _cat[dbName or self._cat.get_current_database_name()]

        return tableName in _cdb.pl_ctx.tables()

    def createExternalTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        **options: str,
    ) -> DataFrame:
        """Creates a table based on the dataset in a data source.

        It returns the DataFrame associated with the external table.

        The data source is specified by the ``source`` and a set of ``options``.
        If ``source`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Optionally, a schema can be provided as the schema of the returned :class:`DataFrame` and
        created external table.

        .. versionadded:: 2.0.0

        Returns
        -------
        :class:`DataFrame`
        """
        warnings.warn(
            "createExternalTable is deprecated since Spark 2.2, please use createTable instead.",
            FutureWarning,
        )
        # return self.createTable(tableName, path, source, schema, **options)
        raise NotImplementedError()

    def createTable(
        self,
        tableName: str,
        path: Optional[str] = None,
        source: Optional[str] = None,
        schema: Optional[StructType] = None,
        description: Optional[str] = None,
        **options: str,
    ) -> DataFrame:
        """Creates a table based on the dataset in a data source.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        tableName : str
            name of the table to create.

            .. versionchanged:: 3.4.0
               Allow ``tableName`` to be qualified with catalog name.

        path : str, optional
            the path in which the data for this table exists.
            When ``path`` is specified, an external table is
            created from the data at the given path. Otherwise a managed table is created.
        source : str, optional
            the source of this table such as 'parquet, 'orc', etc.
            If ``source`` is not specified, the default data source configured by
            ``spark.sql.sources.default`` will be used.
        schema : class:`StructType`, optional
            the schema for this table.
        description : str, optional
            the description of this table.

            .. versionchanged:: 3.1.0
                Added the ``description`` parameter.

        **options : dict, optional
            extra options to specify in the table.

        Returns
        -------
        :class:`DataFrame`
            The DataFrame associated with the table.

        Examples
        --------
        Creating a managed table.

        >>> _ = spark.catalog.createTable("tbl1", schema=spark.range(1).schema, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl1")

        Creating an external table

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.catalog.createTable(
        ...         "tbl2", schema=spark.range(1).schema, path=d, source='parquet')
        >>> _ = spark.sql("DROP TABLE tbl2")
        """
        _path = (
            pathlib.Path(path)
            if path
            else pathlib.Path(self.DEFAULT_SPARK_PATH).joinpath(tableName)
        )
        _path_str = str(_path.absolute())

        source = source or options.get(
            "format", self._sparkSession.conf.get("spark.sql.sources.default")
        )

        # Create empty table
        if not _path.exists():
            if schema:
                df = self._sparkSession.createDataFrame([], schema=schema)
                df.write.format(source).options(**options).save(_path_str)
            else:
                raise ValueError("For empty path schema must be specified")

        # Read existing
        df = load_df_from_location(self._sparkSession, _path_str, source, schema, **options)

        if not schema:
            schema = df.schema

        names = parse_table_name(tableName)
        ts = self._cat.get_ts(tableName)
        if names.table not in ts.tables:
            cols = [tuple(f.simpleString().split(":")) for f in schema.fields]
            ts.tables[names.table] = SourceRelation(
                name=names.table,
                db=names.database,
                format=source,
                location=_path_str,
                columns=cols,
            )
            ts.pl_ctx.register(names.table, df._gather_first())  # noqa

        return df

    def dropTempView(self, viewName: str) -> bool:
        """Drops the local temporary view with the given view name in the catalog.
        If the view has been cached before, then it will also be uncached.
        Returns true if this view is dropped successfully, false otherwise.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        viewName : str
            name of the temporary view to drop.

        Returns
        -------
        bool
            If the temporary view was successfully dropped or not.

            .. versionadded:: 2.1.0
                The return type of this method was ``None`` in Spark 2.0, but changed to ``bool``
                in Spark 2.1.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createTempView("my_table")

        Dropping the temporary view.

        >>> spark.catalog.dropTempView("my_table")
        True

        Throw an exception if the temporary view does not exists.

        >>> spark.name("my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        ts = self._cat.get_ts(viewName)
        ts.pl_ctx.unregister(viewName)
        return True

    def dropGlobalTempView(self, viewName: str) -> bool:
        """Drops the global temporary view with the given view name in the catalog.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        viewName : str
            name of the global view to drop.

        Returns
        -------
        bool
            If the global view was successfully dropped or not.

        Notes
        -----
        If the view has been cached before, then it will also be uncached.

        Examples
        --------
        >>> spark.createDataFrame([(1, 1)]).createGlobalTempView("my_table")

        Dropping the global view.

        >>> spark.catalog.dropGlobalTempView("my_table")
        True

        Throw an exception if the global view does not exists.

        >>> spark.name("global_temp.my_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...
        """
        return self.dropTempView(viewName)

    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: Optional["DataType"] = None
    ) -> "UserDefinedFunctionLike":
        """An alias for :func:`spark.udf.register`.
        See :meth:`pyspark.sql.UDFRegistration.register`.

        .. versionadded:: 2.0.0

        .. deprecated:: 2.3.0
            Use :func:`spark.udf.register` instead.

        .. versionchanged:: 3.4.0
            Supports Spark Connect.
        """
        warnings.warn("Deprecated in 2.3.0. Use spark.udf.register instead.", FutureWarning)
        # return self._sparkSession.udf.register(name, f, returnType)
        raise NotImplementedError

    def isCached(self, tableName: str) -> bool:
        """
        Returns true if the table is currently cached in-memory.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Returns
        -------
        bool

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        True

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.isCached("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.isCached("spark_catalog.default.tbl1")
        True
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._require_table_exists(tableName)

        return tableName in self._cached_tables

    def cacheTable(self, tableName: str, storageLevel: Optional[StorageLevel] = None) -> None:
        """Caches the specified table in-memory or with given storage level.
        Default MEMORY_AND_DISK.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        storageLevel : :class:`StorageLevel`
            storage level to set for persistence.

            .. versionchanged:: 3.5.0
                Allow to specify storage level.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")

        or

        >>> spark.catalog.cacheTable("tbl1", StorageLevel.OFF_HEAP)

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.cacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.cacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        # if storageLevel:
        #     javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
        #     self._jcatalog.cacheTable(tableName, javaStorageLevel)
        # else:
        #     self._jcatalog.cacheTable(tableName)

        self._require_table_exists(tableName)

        ts = self._cat.get_ts(tableName)
        tbl = self._cat.get_table(tableName)

        if tbl.format == "view":
            ldf = tbl.df._gather_first()  # noqa
        else:
            ldf = ts.pl_ctx.execute(f"select * from {tableName}")

        ts.pl_ctx.register(tableName, ldf.collect())
        self._cached_tables[tableName] = ldf

    def uncacheTable(self, tableName: str) -> None:
        """Removes the specified table from the in-memory cache.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.cacheTable("tbl1")
        >>> spark.catalog.uncacheTable("tbl1")
        >>> spark.catalog.isCached("tbl1")
        False

        Throw an analysis exception when the table does not exist.

        >>> spark.catalog.uncacheTable("not_existing_table")
        Traceback (most recent call last):
            ...
        AnalysisException: ...

        Using the fully qualified name for the table.

        >>> spark.catalog.uncacheTable("spark_catalog.default.tbl1")
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        self._require_table_exists(tableName)

        ts = self._cat.get_ts(tableName)
        # Delete materialized pl.DataFrame from the Context
        ts.pl_ctx.unregister(tableName)
        # Get saved pl.LazyFrame and re-register to have it in the Catalog
        ts.pl_ctx.register(tableName, self._cached_tables[tableName])
        # Delete cached link
        self._cached_tables.pop(tableName)

    def clearCache(self) -> None:
        """Removes all cached tables from the in-memory cache.

        .. versionadded:: 2.0.0

        Examples
        --------
        >>> _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        >>> _ = spark.sql("CREATE TABLE tbl1 (name STRING, age INT) USING parquet")
        >>> spark.catalog.clearCache()
        >>> spark.catalog.isCached("tbl1")
        False
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        for t in self._cached_tables.copy():
            self.uncacheTable(t)

    def refreshTable(self, tableName: str) -> None:
        """Invalidates and refreshes all the cached data and metadata of the given table.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        tableName : str
            name of the table to get.

            .. versionchanged:: 3.4.0
                Allow ``tableName`` to be qualified with catalog name.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.name("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.name("tbl1").count()
        1

        After refreshing the table, it shows 0 because the data does not exist anymore.

        >>> spark.catalog.refreshTable("tbl1")
        >>> spark.name("tbl1").count()
        0

        Using the fully qualified name for the table.

        >>> spark.catalog.refreshTable("spark_catalog.default.tbl1")
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        pass

    def recoverPartitions(self, tableName: str) -> None:
        """Recovers all the partitions of the given table and updates the catalog.

        .. versionadded:: 2.1.1

        Parameters
        ----------
        tableName : str
            name of the table to get.

        Notes
        -----
        Only works with a partitioned table, and not a view.

        Examples
        --------
        The example below creates a partitioned table against the existing directory of
        the partitioned table. After that, it recovers the partitions.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     spark.range(1).selectExpr(
        ...         "id as key", "id as value").write.partitionBy("key").mode("overwrite").save(d)
        ...     _ = spark.sql(
        ...          "CREATE TABLE tbl1 (key LONG, value LONG)"
        ...          "USING parquet OPTIONS (path '{}') PARTITIONED BY (key)".format(d))
        ...     spark.name("tbl1").show()
        ...     spark.catalog.recoverPartitions("tbl1")
        ...     spark.name("tbl1").show()
        +-----+---+
        |value|key|
        +-----+---+
        +-----+---+
        +-----+---+
        |value|key|
        +-----+---+
        |    0|  0|
        +-----+---+
        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def refreshByPath(self, path: str) -> None:
        """Invalidates and refreshes all the cached data (and the associated metadata) for any
        DataFrame that contains the given data source path.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        path : str
            the path to refresh the cache.

        Examples
        --------
        The example below caches a table, and then removes the data.

        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     _ = spark.sql("DROP TABLE IF EXISTS tbl1")
        ...     _ = spark.sql(
        ...         "CREATE TABLE tbl1 (col STRING) USING TEXT LOCATION '{}'".format(d))
        ...     _ = spark.sql("INSERT INTO tbl1 SELECT 'abc'")
        ...     spark.catalog.cacheTable("tbl1")
        ...     spark.name("tbl1").show()
        +---+
        |col|
        +---+
        |abc|
        +---+

        Because the table is cached, it computes from the cached data as below.

        >>> spark.name("tbl1").count()
        1

        After refreshing the table by path, it shows 0 because the data does not exist anymore.

        >>> spark.catalog.refreshByPath(d)
        >>> spark.name("tbl1").count()
        0

        >>> _ = spark.sql("DROP TABLE tbl1")
        """
        raise NotImplementedError()

    def _reset(self) -> None:
        """(Internal use only) Drop all existing databases (except "default"), tables,
        partitions and functions, and set the current database to "default".

        This is mainly used for tests.
        """
        self._jsparkSession.sessionState().catalog().reset()

    @staticmethod
    def _regex_pattern(pattern: str):
        return "^" + re.escape(pattern).replace(r"\*", ".*") + "$"

    def _require_table_exists(self, table_name: str):
        tbl = self._cat.get_table(table_name)
        # if tableName not in ts.pl_ctx.tables():
        if tbl is None:
            raise AnalysisException(f"Table {table_name} does not exist")


def _test() -> None:
    import os
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    globs["spark"] = (
        SparkSession.builder.master("local[4]").appName("sql.catalog tests").getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.catalog,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )
    globs["spark"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
