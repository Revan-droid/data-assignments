from __future__ import annotations

import json
from typing import Any

from .models import ChangeEvent
from .postgres import PostgresConnection
from .util import stable_json, utc_now_iso

SCHEMA = "lake"


class LakeDatabase:
    def __init__(self, conn: PostgresConnection):
        self.conn = conn

    def close(self) -> None:
        return

    def initialize(self) -> None:
        with self.conn.transaction():
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.cdc_events (
                    change_id BIGINT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    op TEXT NOT NULL,
                    entity_key JSONB NOT NULL,
                    before_json JSONB,
                    after_json JSONB,
                    source_commit_ts TIMESTAMPTZ NOT NULL,
                    source_txn_id TEXT NOT NULL,
                    source_schema_hash TEXT NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_lake_events_table_change_id ON {SCHEMA}.cdc_events(table_name, change_id)")

    def has_change(self, change_id: int) -> bool:
        return int(self.conn.scalar(f"SELECT COUNT(*) FROM {SCHEMA}.cdc_events WHERE change_id = ?", (change_id,), default=0)) > 0

    def append(self, change: ChangeEvent) -> None:
        if self.has_change(change.change_id):
            return
        self.conn.execute(
            f"""
            INSERT INTO {SCHEMA}.cdc_events (
                change_id, table_name, op, entity_key, before_json, after_json, source_commit_ts, source_txn_id, source_schema_hash, ingested_at
            ) VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?, ?, ?)
            """,
            (
                change.change_id,
                change.table_name,
                change.op,
                stable_json(change.entity_key),
                None if change.before is None else stable_json(change.before),
                None if change.after is None else stable_json(change.after),
                change.source_commit_ts,
                change.source_txn_id,
                change.source_schema_hash,
                utc_now_iso(),
            ),
        )

    def count(self) -> int:
        return int(self.conn.scalar(f"SELECT COUNT(*) FROM {SCHEMA}.cdc_events", default=0))

