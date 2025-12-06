from dataclasses import dataclass
from typing import Dict

import polars as pl

from polarspark.sql._internal.catalog.utils import parse_table_name
from polarspark.sql._internal.parser.models import CreateTable


@dataclass
class TableStore:
    pl_ctx: pl.SQLContext
    tables: Dict[str, CreateTable]


class InMemoryCatalog:
    _default_catalog = "spark_catalog"
    _default_database = "default"

    def __init__(self):
        self._catalogs = {
            self._default_catalog: {
                self._default_database: TableStore(pl.SQLContext(), {})
            }
        }
        self._current_catalog = self._default_catalog
        self._current_database = self._default_database

    def add_table(self, table: CreateTable):
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

    def get_table(self, table_name: str):
        names = parse_table_name(table_name)
        ts = self.get_ts(table_name)
        return ts.tables.get(names.table)

    def catalogs(self):
        return self._catalogs

    def get_ts(self, table_name: str):
        names = parse_table_name(table_name)
        _cat_name = names.catalog or self.get_current_catalog_name()
        _db_name = names.database or self.get_current_database_name()
        dbs = self._catalogs[_cat_name]
        return dbs[_db_name]

