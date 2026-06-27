from __future__ import annotations

import argparse
import os
from pathlib import Path

from .catalog import Catalog
from .lake import LakeDatabase
from .pipeline import CdcPipeline, PipelineControl
from .postgres import PostgresConnection
from .server import serve
from .source import SourceDatabase
from .warehouse import WarehouseDatabase


def open_connection() -> PostgresConnection:
    return PostgresConnection(
        host=os.getenv("CDC_PGHOST", "127.0.0.1"),
        port=int(os.getenv("CDC_PGPORT", "5432")),
        user=os.getenv("CDC_PGUSER", "postgres"),
        database=os.getenv("CDC_PGDATABASE", "cdc_lakehouse"),
        password=os.getenv("CDC_PGPASSWORD"),
    )


def build_workspace() -> tuple[PostgresConnection, SourceDatabase, LakeDatabase, WarehouseDatabase, PipelineControl]:
    conn = open_connection()
    source = SourceDatabase(conn)
    lake = LakeDatabase(conn)
    warehouse = WarehouseDatabase(conn)
    control = PipelineControl(conn)
    source.initialize()
    lake.initialize()
    warehouse.initialize()
    control.initialize()
    return conn, source, lake, warehouse, control


def run_init() -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        pipeline = CdcPipeline(source, lake, warehouse, control)
        pipeline.bootstrap_contract()
        print("initialized source, lake, warehouse, and control schemas")
    finally:
        conn.close()


def run_seed() -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        source.seed_demo_data()
        print("seeded demo source data")
    finally:
        conn.close()


def run_sync() -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        pipeline = CdcPipeline(source, lake, warehouse, control)
        count = pipeline.run_once()
        Catalog(None).build(source, lake, warehouse)
        print(f"synchronized {count} change events")
    finally:
        conn.close()


def run_validate() -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        print(warehouse.validate())
    finally:
        conn.close()


def run_snapshot(as_of_ts: str) -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        print(warehouse.create_point_in_time_snapshot(as_of_ts))
    finally:
        conn.close()


def run_break_schema() -> None:
    conn, source, lake, warehouse, control = build_workspace()
    try:
        source.introduce_breaking_schema_change()
        print("introduced incompatible schema drift")
    finally:
        conn.close()


def run_serve() -> None:
    serve()


def main() -> None:
    parser = argparse.ArgumentParser(description="CDC lakehouse reliability assignment")
    parser.add_argument("--base-dir", type=Path, default=Path("."))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("seed")
    sub.add_parser("sync")
    sub.add_parser("validate")
    snap = sub.add_parser("snapshot")
    snap.add_argument("--as-of", required=True)
    sub.add_parser("break-schema")
    sub.add_parser("serve")
    args = parser.parse_args()

    if args.command == "init":
        run_init()
    elif args.command == "seed":
        run_seed()
    elif args.command == "sync":
        run_sync()
    elif args.command == "validate":
        run_validate()
    elif args.command == "snapshot":
        run_snapshot(args.as_of)
    elif args.command == "break-schema":
        run_break_schema()
    elif args.command == "serve":
        run_serve()


if __name__ == "__main__":
    main()

