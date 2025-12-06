from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CreateTable:
        name: str
        db: str
        format: str
        columns: Optional[list[tuple[str]]] = field(default_factory=list)
        partitioned_by: Optional[list[str]] = None
        location: Optional[str] = None


@dataclass
class InsertInto:
        table: str
        values: list[str]
        columns: Optional[list[str]] = field(default_factory=list)
        db: Optional[str] = None


@dataclass
class SelectFrom:
    table: Optional[str] = None
    db: Optional[str] = None


@dataclass
class DropTable:
    table: Optional[str] = None
    db: Optional[str] = None

