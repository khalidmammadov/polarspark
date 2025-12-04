from typing import List

from polarspark.sql._internal.parser.models import CreateTable

class InMemoryCatalog:
    _default_catalog = "spark_catalog"
    _default_database = "default"

    def __init__(self):
        self._catalogs = {
            self._default_catalog: {
                self._default_database: {}
            }
        }
        self._current_catalog = self._default_catalog
        self._current_database = self._default_database

    def add_table(self, table: CreateTable):
        cat = self._catalogs[self._default_catalog]
        db = cat[table.db]
        db[table.name] = table

    def set_current_database(self, database: str):
        self._current_database = database

    def set_current_catalog(self, catalog: str):
        self._current_catalog = catalog

    def get_current_database(self) -> str:
        return self._current_database

    def get_current_catalog(self) -> str:
        return self._current_catalog
        return self._current_catalog

    def get_table(self, table_name: str):
        return self._catalogs[self._default_catalog][table_name]

    def catalogs(self):
        return self._catalogs


