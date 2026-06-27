from __future__ import annotations

from typing import Any

from .exceptions import ValidationError
from .models import ChangeEvent
from .postgres import PostgresConnection
from .util import stable_json, utc_now_iso

SCHEMA = "warehouse"


class WarehouseDatabase:
    TABLE_DEFS: dict[str, list[tuple[str, str]]] = {
        "customers": [
            ("customer_id", "BIGINT"),
            ("external_customer_ref", "TEXT"),
            ("email", "TEXT"),
            ("full_name", "TEXT"),
            ("phone", "TEXT"),
            ("customer_status", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "customer_addresses": [
            ("address_id", "BIGINT"),
            ("customer_id", "BIGINT"),
            ("address_type", "TEXT"),
            ("line1", "TEXT"),
            ("line2", "TEXT"),
            ("city", "TEXT"),
            ("state", "TEXT"),
            ("postal_code", "TEXT"),
            ("country_code", "TEXT"),
            ("is_default", "INTEGER"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "products": [
            ("product_id", "BIGINT"),
            ("sku", "TEXT"),
            ("product_name", "TEXT"),
            ("category", "TEXT"),
            ("unit_price", "NUMERIC"),
            ("product_status", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "orders": [
            ("order_id", "BIGINT"),
            ("customer_id", "BIGINT"),
            ("order_number", "TEXT"),
            ("order_status", "TEXT"),
            ("currency_code", "TEXT"),
            ("subtotal", "NUMERIC"),
            ("tax_amount", "NUMERIC"),
            ("shipping_amount", "NUMERIC"),
            ("total_amount", "NUMERIC"),
            ("placed_at", "TEXT"),
            ("cancelled_at", "TEXT"),
            ("billing_address_id", "BIGINT"),
            ("shipping_address_id", "BIGINT"),
            ("notes", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "order_items": [
            ("order_id", "BIGINT"),
            ("line_number", "INTEGER"),
            ("product_id", "BIGINT"),
            ("quantity", "INTEGER"),
            ("unit_price", "NUMERIC"),
            ("discount_amount", "NUMERIC"),
            ("line_total", "NUMERIC"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "payments": [
            ("payment_id", "BIGINT"),
            ("order_id", "BIGINT"),
            ("payment_method", "TEXT"),
            ("payment_status", "TEXT"),
            ("amount", "NUMERIC"),
            ("provider_reference", "TEXT"),
            ("authorized_at", "TEXT"),
            ("captured_at", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
        "payment_attempts": [
            ("payment_attempt_id", "BIGINT"),
            ("payment_id", "BIGINT"),
            ("attempt_no", "INTEGER"),
            ("attempt_status", "TEXT"),
            ("decline_code", "TEXT"),
            ("gateway_latency_ms", "INTEGER"),
            ("attempted_at", "TEXT"),
            ("request_id", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ],
    }

    ENUMS: dict[str, dict[str, set[str]]] = {
        "customers": {"customer_status": {"active", "suspended", "closed"}},
        "customer_addresses": {"address_type": {"billing", "shipping"}},
        "products": {"product_status": {"active", "discontinued"}},
        "orders": {"order_status": {"draft", "placed", "paid", "shipped", "completed", "cancelled", "refunded"}},
        "payments": {"payment_method": {"card", "bank_transfer", "wallet"}, "payment_status": {"pending", "authorized", "captured", "failed", "refunded"}},
        "payment_attempts": {"attempt_status": {"started", "succeeded", "failed"}},
    }

    REQUIRED_NON_NULL: dict[str, tuple[str, ...]] = {
        "customers": ("customer_id", "external_customer_ref", "email", "full_name", "customer_status"),
        "customer_addresses": ("address_id", "customer_id", "address_type", "line1", "city", "state", "postal_code", "country_code", "is_default"),
        "products": ("product_id", "sku", "product_name", "category", "unit_price", "product_status"),
        "orders": ("order_id", "customer_id", "order_number", "order_status", "currency_code", "subtotal", "tax_amount", "shipping_amount", "total_amount", "placed_at"),
        "order_items": ("order_id", "line_number", "product_id", "quantity", "unit_price", "discount_amount", "line_total"),
        "payments": ("payment_id", "order_id", "payment_method", "payment_status", "amount"),
        "payment_attempts": ("payment_attempt_id", "payment_id", "attempt_no", "attempt_status", "attempted_at", "request_id"),
    }

    def __init__(self, conn: PostgresConnection):
        self.conn = conn

    def close(self) -> None:
        return

    @staticmethod
    def _pk_columns(table_name: str) -> list[str]:
        return {
            "customers": ["customer_id"],
            "customer_addresses": ["address_id"],
            "products": ["product_id"],
            "orders": ["order_id"],
            "order_items": ["order_id", "line_number"],
            "payments": ["payment_id"],
            "payment_attempts": ["payment_attempt_id"],
        }[table_name]

    def initialize(self) -> None:
        with self.conn.transaction():
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.applied_changes (
                    change_id BIGINT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL,
                    table_name TEXT NOT NULL,
                    op TEXT NOT NULL
                )
                """
            )
            for table_name, cols in self.TABLE_DEFS.items():
                history_name = f"{table_name}_history"
                col_sql = ", ".join(f"{name} {dtype}" for name, dtype in cols)
                pk_expr = ", ".join(self._pk_columns(table_name))
                self.conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA}.{history_name} (
                        {col_sql},
                        valid_from TIMESTAMPTZ NOT NULL,
                        valid_to TIMESTAMPTZ,
                        is_current INTEGER NOT NULL CHECK (is_current IN (0,1)),
                        source_change_id BIGINT NOT NULL UNIQUE,
                        source_op TEXT NOT NULL,
                        source_commit_ts TIMESTAMPTZ NOT NULL,
                        ingested_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY ({pk_expr}, source_change_id)
                    )
                    """
                )
                self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{history_name}_current ON {SCHEMA}.{history_name}({pk_expr}, is_current)")
                self.conn.execute(f"CREATE OR REPLACE VIEW {SCHEMA}.{table_name}_current AS SELECT * FROM {SCHEMA}.{history_name} WHERE is_current = 1")
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.validation_runs (
                    run_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    run_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    report_json JSONB NOT NULL
                )
                """
            )
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA}.restore_snapshots (
                    snapshot_name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    as_of_ts TIMESTAMPTZ NOT NULL,
                    table_name TEXT NOT NULL,
                    row_count BIGINT NOT NULL,
                    PRIMARY KEY (snapshot_name, table_name)
                )
                """
            )

    def has_applied(self, change_id: int) -> bool:
        return int(self.conn.scalar(f"SELECT COUNT(*) FROM {SCHEMA}.applied_changes WHERE change_id = ?", (change_id,), default=0)) > 0

    def _insert_history(self, table_name: str, record: dict[str, Any], change: ChangeEvent) -> None:
        cols = [name for name, _ in self.TABLE_DEFS[table_name]]
        values = [record.get(col) for col in cols]
        self.conn.execute(
            f"""
            INSERT INTO {SCHEMA}.{table_name}_history (
                {", ".join(cols)},
                valid_from, valid_to, is_current, source_change_id, source_op, source_commit_ts, ingested_at
            ) VALUES (
                {", ".join("?" for _ in cols)},
                ?, ?, ?, ?, ?, ?, ?
            )
            """,
            values + [change.source_commit_ts, None, 1, change.change_id, change.op, change.source_commit_ts, utc_now_iso()],
        )

    def _close_current(self, table_name: str, record: dict[str, Any] | None) -> None:
        if record is None:
            return
        pk_values = [record[col] for col in self._pk_columns(table_name)]
        self.conn.execute(
            f"""
            UPDATE {SCHEMA}.{table_name}_history
            SET valid_to = ?, is_current = 0
            WHERE {" AND ".join(f"{col} = ?" for col in self._pk_columns(table_name))} AND is_current = 1
            """,
            [utc_now_iso(), *pk_values],
        )

    def _validate_row(self, table_name: str, record: dict[str, Any]) -> None:
        for col in self.REQUIRED_NON_NULL[table_name]:
            if record.get(col) is None:
                raise ValidationError(f"warehouse null violation: {table_name}.{col}")
        for column, allowed in self.ENUMS.get(table_name, {}).items():
            if record.get(column) not in allowed:
                raise ValidationError(f"warehouse enum violation: {table_name}.{column}={record.get(column)}")
        for field in ("unit_price", "subtotal", "tax_amount", "shipping_amount", "total_amount", "amount", "discount_amount", "line_total"):
            if field in record and record[field] is not None and float(record[field]) < 0:
                raise ValidationError(f"warehouse numeric violation: {table_name}.{field}")
        if table_name == "orders" and (record["currency_code"] is None or len(str(record["currency_code"])) != 3):
            raise ValidationError("warehouse order currency_code invalid")

    def apply_change(self, change: ChangeEvent) -> None:
        if self.has_applied(change.change_id):
            return
        if change.table_name not in self.TABLE_DEFS:
            raise ValidationError(f"unsupported table in warehouse: {change.table_name}")
        if change.op == "I":
            self._validate_row(change.table_name, change.after or {})
            self._insert_history(change.table_name, change.after or {}, change)
        elif change.op == "U":
            self._validate_row(change.table_name, change.after or {})
            self._close_current(change.table_name, change.after)
            self._insert_history(change.table_name, change.after or {}, change)
        elif change.op == "D":
            self._close_current(change.table_name, change.before)
        else:
            raise ValidationError(f"unsupported op: {change.op}")
        self.conn.execute(
            f"INSERT INTO {SCHEMA}.applied_changes(change_id, applied_at, table_name, op) VALUES (?, ?, ?, ?)",
            (change.change_id, utc_now_iso(), change.table_name, change.op),
        )

    def validate(self) -> dict[str, Any]:
        failures: list[str] = []
        checks: list[tuple[str, bool]] = []
        try:
            self._validate_referential_integrity()
            checks.append(("referential_integrity", True))
        except ValidationError as exc:
            checks.append(("referential_integrity", False))
            failures.append(str(exc))
        try:
            self._validate_business_rules()
            checks.append(("business_rules", True))
        except ValidationError as exc:
            checks.append(("business_rules", False))
            failures.append(str(exc))
        report = {"status": "pass" if not failures else "fail", "checks": [{"name": n, "pass": p} for n, p in checks], "failures": failures}
        self.conn.execute(
            f"INSERT INTO {SCHEMA}.validation_runs(run_at, status, report_json) VALUES (?, ?, ?::jsonb)",
            (utc_now_iso(), report["status"], stable_json(report)),
        )
        if failures:
            raise ValidationError("; ".join(failures))
        return report

    def _validate_referential_integrity(self) -> None:
        customer_ids = {row["customer_id"] for row in self.conn.query(f"SELECT customer_id FROM {SCHEMA}.customers_current").fetchall()}
        product_ids = {row["product_id"] for row in self.conn.query(f"SELECT product_id FROM {SCHEMA}.products_current").fetchall()}
        order_ids = {row["order_id"] for row in self.conn.query(f"SELECT order_id FROM {SCHEMA}.orders_current").fetchall()}
        payment_ids = {row["payment_id"] for row in self.conn.query(f"SELECT payment_id FROM {SCHEMA}.payments_current").fetchall()}
        for row in self.conn.query(f"SELECT order_id, customer_id FROM {SCHEMA}.orders_current").fetchall():
            if row["customer_id"] not in customer_ids:
                raise ValidationError(f"orders.customer_id missing for order_id={row['order_id']}")
        for row in self.conn.query(f"SELECT order_id, line_number, product_id FROM {SCHEMA}.order_items_current").fetchall():
            if row["order_id"] not in order_ids:
                raise ValidationError(f"order_items.order_id missing for order_id={row['order_id']}")
            if row["product_id"] not in product_ids:
                raise ValidationError(f"order_items.product_id missing for line={row['line_number']}")
        for row in self.conn.query(f"SELECT payment_id, order_id FROM {SCHEMA}.payments_current").fetchall():
            if row["order_id"] not in order_ids:
                raise ValidationError(f"payments.order_id missing for payment_id={row['payment_id']}")
        for row in self.conn.query(f"SELECT payment_attempt_id, payment_id FROM {SCHEMA}.payment_attempts_current").fetchall():
            if row["payment_id"] not in payment_ids:
                raise ValidationError(f"payment_attempts.payment_id missing for attempt_id={row['payment_attempt_id']}")

    def _validate_business_rules(self) -> None:
        for row in self.conn.query(f"SELECT * FROM {SCHEMA}.orders_current").fetchall():
            if row["order_status"] in {"placed", "paid", "shipped", "completed", "refunded"}:
                items_total = float(self.conn.scalar(f"SELECT COALESCE(SUM(line_total), 0) FROM {SCHEMA}.order_items_current WHERE order_id = ?", (row["order_id"],), default=0))
                expected = round(items_total + float(row["tax_amount"]) + float(row["shipping_amount"]), 2)
                if round(float(row["total_amount"]), 2) != expected:
                    raise ValidationError(f"order total mismatch for order_id={row['order_id']}")
            if row["cancelled_at"] is not None and row["order_status"] != "cancelled":
                raise ValidationError(f"cancelled_at set without cancelled status for order_id={row['order_id']}")
        for row in self.conn.query(f"SELECT * FROM {SCHEMA}.order_items_current").fetchall():
            if int(row["quantity"]) <= 0:
                raise ValidationError(f"order_item quantity invalid for order_id={row['order_id']}")
        for row in self.conn.query(f"SELECT * FROM {SCHEMA}.payments_current").fetchall():
            if float(row["amount"]) <= 0:
                raise ValidationError(f"payment amount invalid for payment_id={row['payment_id']}")

    def create_point_in_time_snapshot(self, as_of_ts: str) -> dict[str, int]:
        snapshot_name = f"snapshot_{as_of_ts.replace(':', '').replace('-', '').replace('+', '').replace('T', '_')}"
        counts: dict[str, int] = {}
        for table_name, cols in self.TABLE_DEFS.items():
            snapshot_table = f"{snapshot_name}_{table_name}"
            column_names = [name for name, _ in cols]
            self.conn.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{snapshot_table}")
            self.conn.execute(
                f"""
                CREATE TABLE {SCHEMA}.{snapshot_table} AS
                SELECT {", ".join(column_names)}
                FROM {SCHEMA}.{table_name}_history
                WHERE valid_from <= ?
                  AND (valid_to IS NULL OR valid_to > ?)
                """,
                (as_of_ts, as_of_ts),
            )
            counts[table_name] = int(self.conn.scalar(f"SELECT COUNT(*) FROM {SCHEMA}.{snapshot_table}", default=0))
            self.conn.execute(
                f"""
                INSERT INTO {SCHEMA}.restore_snapshots(snapshot_name, created_at, as_of_ts, table_name, row_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (snapshot_name, table_name)
                DO UPDATE SET created_at = EXCLUDED.created_at, as_of_ts = EXCLUDED.as_of_ts, row_count = EXCLUDED.row_count
                """,
                (snapshot_name, utc_now_iso(), as_of_ts, table_name, counts[table_name]),
            )
        return counts

    def current_counts(self) -> dict[str, int]:
        return {table: int(self.conn.scalar(f"SELECT COUNT(*) FROM {SCHEMA}.{table}_current", default=0)) for table in self.TABLE_DEFS}

