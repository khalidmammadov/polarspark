from dataclasses import dataclass
from functools import reduce
from typing import Dict

import polars as pl

from polarspark.sql._internal.catalog.utils import parse_table_name
from polarspark.sql._internal.parser.models import SourceRelation
from polarspark.sql.dataframe import DataFrame


@dataclass
class TableStore:
    pl_ctx: pl.SQLContext
    tables: Dict[str, SourceRelation]


class InMemoryCatalog:
    _default_catalog = "spark_catalog"
    _default_database = "default"

    def __init__(self):
        self._catalogs = {
            self._default_catalog: {self._default_database: TableStore(pl.SQLContext(), {})}
        }
        self._current_catalog = self._default_catalog
        self._current_database = self._default_database

    def add_table(self, table: SourceRelation) -> None:
        cat = self._catalogs[self._default_catalog]
        ts = cat[table.db]
        ts.tables[table.name] = table

    def set_current_database(self, database: str):
        self._current_database = database

    def set_current_catalog(self, catalog: str):
        self._current_catalog = catalog

    def get_current_database_name(self) -> str:
        return self._current_database

    def get_current_database(self) -> TableStore:
        _cat = self.get_current_catalog()
        return _cat[self.get_current_database_name()]

    def get_current_catalog_name(self) -> str:
        return self._current_catalog

    def get_current_catalog(self) -> Dict[str, TableStore]:
        return self._catalogs[self._current_catalog]

    def get_table(self, table_name: str) -> SourceRelation:
        names = parse_table_name(table_name)
        ts = self.get_ts(table_name)
        return ts.tables.get(names.table)

    def catalogs(self) -> dict:
        return self._catalogs

    def get_ts(self, table_name: str) -> TableStore:
        names = parse_table_name(table_name)
        _cat_name = names.catalog or self.get_current_catalog_name()
        _db_name = names.database or self.get_current_database_name()
        dbs = self._catalogs[_cat_name]
        return dbs[_db_name]

    def create_or_append_in_mem_table(
        self, table_name: str, ldf: pl.LazyFrame, is_streaming: bool = False
    ) -> None:
        names = parse_table_name(table_name)
        ts = self.get_ts(table_name)
        if t := ts.tables.get(names.table):
            t.data.append(ldf)
        else:
            t = SourceRelation(
                name=names.table,
                db=names.database,
                format="memory",
                data=[ldf],
                is_streaming=is_streaming,
            )
            ts.tables[table_name] = t

        _ldf = pl.concat(t.data, how="vertical")

        ts.pl_ctx.register(names.table, _ldf)  # noqa

    def create_or_replace_tmp_view_relation(
        self, view_name: str, df: "DataFrame", is_streaming: bool = False
    ) -> None:
        names = parse_table_name(view_name)
        ts = self.get_ts(view_name)
        if t := ts.tables.get(names.table):
            t.df = df
        else:
            t = SourceRelation(
                name=names.table, db=names.database, format="view", is_streaming=is_streaming, df=df
            )
            ts.tables[view_name] = t
