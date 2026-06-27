from __future__ import annotations

import os
import unittest

from cdc_lakehouse.postgres import quote_literal, render_sql


class PostgresHelperTests(unittest.TestCase):
    def test_quote_literal(self) -> None:
        self.assertEqual(quote_literal(None), "NULL")
        self.assertEqual(quote_literal("O'Reilly"), "'O''Reilly'")
        self.assertEqual(quote_literal(True), "TRUE")
        self.assertEqual(quote_literal({"b": 2, "a": 1}), '\'{"a":1,"b":2}\'')

    def test_render_sql(self) -> None:
        sql = render_sql("SELECT * FROM foo WHERE id = ? AND status = ?", [10, "active"])
        self.assertEqual(sql, "SELECT * FROM foo WHERE id = 10 AND status = 'active'")


@unittest.skipUnless(os.getenv("CDC_TEST_PGHOST"), "set CDC_TEST_PGHOST to run Postgres integration tests")
class PostgresIntegrationTests(unittest.TestCase):
    def test_connection_round_trip(self) -> None:
        from cdc_lakehouse.postgres import PostgresConnection

        conn = PostgresConnection(
            host=os.environ["CDC_TEST_PGHOST"],
            port=int(os.getenv("CDC_TEST_PGPORT", "5432")),
            user=os.getenv("CDC_TEST_PGUSER", "postgres"),
            database=os.getenv("CDC_TEST_PGDATABASE", "cdc_lakehouse"),
            password=os.getenv("CDC_TEST_PGPASSWORD"),
        )
        try:
            row = conn.query("SELECT 1 AS one").fetchone()
            self.assertEqual(row["one"], "1")
        finally:
            conn.close()

