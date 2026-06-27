from __future__ import annotations

import json
from pathlib import Path

from .lake import LakeDatabase
from .source import SourceDatabase
from .util import utc_now_iso
from .warehouse import WarehouseDatabase


class Catalog:
    def __init__(self, path: Path | None = None):
        self.path = path

    def build(self, source: SourceDatabase, lake: LakeDatabase, warehouse: WarehouseDatabase) -> dict[str, object]:
        schema_hash = source.schema_fingerprint()
        datasets = [
            {
                "name": "lake.cdc_events",
                "layer": "lake",
                "format": "postgres-table",
                "owner": "data-platform",
                "description": "Append-only capture of every source change with before/after payloads.",
                "intended_users": ["data-engineering", "audit", "replay"],
                "refresh_cadence": "near-real-time",
                "primary_keys": ["change_id"],
                "source_schema_hash": schema_hash,
                "row_count": lake.count(),
            }
        ]
        for table in warehouse.TABLE_DEFS:
            datasets.append(
                {
                    "name": f"warehouse.{table}_current",
                    "layer": "warehouse",
                    "format": "postgres-view",
                    "owner": "analytics-engineering",
                    "description": f"Latest snapshot of {table}.",
                    "intended_users": ["analytics", "bi", "data-science"],
                    "refresh_cadence": "near-real-time",
                    "primary_keys": warehouse._pk_columns(table),
                    "source_schema_hash": schema_hash,
                }
            )
            datasets.append(
                {
                    "name": f"warehouse.{table}_history",
                    "layer": "warehouse",
                    "format": "postgres-table",
                    "owner": "analytics-engineering",
                    "description": f"Temporal history for {table} with restore support.",
                    "intended_users": ["analytics", "audit", "recovery"],
                    "refresh_cadence": "near-real-time",
                    "primary_keys": warehouse._pk_columns(table) + ["source_change_id"],
                    "source_schema_hash": schema_hash,
                }
            )
        catalog = {"published_at": utc_now_iso(), "datasets": datasets}
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(catalog, indent=2, sort_keys=True), encoding="utf-8")
        return catalog

