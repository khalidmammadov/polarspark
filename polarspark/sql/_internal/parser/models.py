from dataclasses import dataclass


@dataclass
class CreateTable:
        name: str
        db: str
        columns: list[tuple[str]]
        format: str
        partitioned_by: list[str]
        location: str


@dataclass
class InsertInto:
        table: str
        db: str
        columns: list[str]
        values: list[str]
