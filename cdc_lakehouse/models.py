from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChangeEvent:
    change_id: int
    table_name: str
    op: str
    entity_key: dict[str, Any]
    source_commit_ts: str
    source_txn_id: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_schema_hash: str


@dataclass(frozen=True)
class SchemaColumn:
    table_name: str
    column_name: str
    data_type: str
    notnull: bool
    is_pk: bool
    default_value: str | None
    ordinal: int


@dataclass(frozen=True)
class TableContract:
    table_name: str
    columns: tuple[SchemaColumn, ...]
    foreign_keys: tuple[tuple[str, str, str], ...]
    indexes: tuple[str, ...]

