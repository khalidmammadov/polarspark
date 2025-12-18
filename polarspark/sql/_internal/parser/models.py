from dataclasses import dataclass, field
from typing import Optional

import polars as pl


@dataclass
class SourceRelation:
    name: str
    format: Optional[str]
    db: Optional[str] = None
    columns: Optional[list[tuple[str]]] = field(default_factory=list)
    partitioned_by: Optional[list[str]] = None
    location: Optional[str] = None
    data: Optional[list[pl.LazyFrame]] = field(default_factory=list)
    is_streaming: bool = False
    df: Optional["DataFrame"] = None


@dataclass
class InsertInto:
    table: str
    values: list[str]
    columns: Optional[list[str]] = field(default_factory=list)
    db: Optional[str] = None


@dataclass
class SelectFrom:
    tables: Optional[list[str]] = None


@dataclass
class DropTable:
    table: Optional[str] = None
    db: Optional[str] = None
