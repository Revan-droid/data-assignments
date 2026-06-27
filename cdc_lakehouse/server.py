from __future__ import annotations

import html
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .catalog import Catalog
from .lake import LakeDatabase
from .pipeline import CdcPipeline, PipelineControl
from .postgres import PostgresConnection
from .source import SourceDatabase
from .warehouse import WarehouseDatabase


class AppState:
    def __init__(self):
        self.conn = PostgresConnection(
            host=os.getenv("CDC_PGHOST", "127.0.0.1"),
            port=int(os.getenv("CDC_PGPORT", "5432")),
            user=os.getenv("CDC_PGUSER", "postgres"),
            database=os.getenv("CDC_PGDATABASE", "cdc_lakehouse"),
            password=os.getenv("CDC_PGPASSWORD"),
        )
        self.source = SourceDatabase(self.conn)
        self.lake = LakeDatabase(self.conn)
        self.warehouse = WarehouseDatabase(self.conn)
        self.control = PipelineControl(self.conn)
        self.source.initialize()
        self.lake.initialize()
        self.warehouse.initialize()
        self.control.initialize()
        self.pipeline = CdcPipeline(self.source, self.lake, self.warehouse, self.control)

    def close(self) -> None:
        self.conn.close()

    def summary(self) -> dict[str, object]:
        return {
            "pipeline_state": self.control.state(),
            "source_counts": self.source.counts(),
            "lake_count": self.lake.count(),
            "warehouse_counts": self.warehouse.current_counts(),
            "validation_runs": int(self.conn.scalar("SELECT COUNT(*) FROM warehouse.validation_runs", default=0)),
            "drift_alerts": int(self.conn.scalar("SELECT COUNT(*) FROM control.drift_alerts", default=0)),
            "catalog": Catalog(None).build(self.source, self.lake, self.warehouse),
        }


def _render_page(title: str, body: str) -> str:
    return f"""
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{html.escape(title)}</title>
        <style>
          :root {{
            --bg: #0f1220;
            --panel: #171b2e;
            --panel-2: #1f2540;
            --text: #f2f5ff;
            --muted: #aab2d5;
            --accent: #78d6ff;
            --accent-2: #8df0b4;
            --danger: #ff8a8a;
          }}
          body {{
            margin: 0;
            font-family: Inter, ui-sans-serif, system-ui, sans-serif;
            background: radial-gradient(circle at top, #1a2140, var(--bg) 50%);
            color: var(--text);
          }}
          a {{ color: var(--accent); text-decoration: none; }}
          .wrap {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
          .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }}
          .card {{ background: rgba(23,27,46,0.92); border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 16px; box-shadow: 0 12px 30px rgba(0,0,0,0.22); }}
          .card h3, .card h2 {{ margin-top: 0; }}
          .muted {{ color: var(--muted); }}
          pre {{
            white-space: pre-wrap;
            word-break: break-word;
            background: rgba(255,255,255,0.04);
            padding: 12px;
            border-radius: 12px;
            overflow: auto;
          }}
          table {{ width: 100%; border-collapse: collapse; }}
          th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,0.08); vertical-align: top; }}
          th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }}
          input, select, button {{
            background: #0f1326;
            color: var(--text);
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 10px;
            padding: 10px 12px;
            font: inherit;
          }}
          button {{ cursor: pointer; background: linear-gradient(135deg, #2a66ff, #0db3ff); border: 0; }}
          .row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
          .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: rgba(255,255,255,0.08); margin-right: 6px; }}
          .nav {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }}
          .nav a {{ padding: 8px 12px; background: rgba(255,255,255,0.06); border-radius: 999px; }}
          .danger {{ color: var(--danger); }}
          .ok {{ color: var(--accent-2); }}
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="nav">
            <a href="/">Overview</a>
            <a href="/state">State JSON</a>
            <a href="/changes">All Changes</a>
            <a href="/catalog">Catalog</a>
            <a href="/source/customers">Source Customers</a>
            <a href="/warehouse/customers/current">Warehouse Current</a>
            <a href="/warehouse/customers/history">Warehouse History</a>
          </div>
          {body}
        </div>
      </body>
    </html>
    """


def _table_html(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "<p class='muted'>No rows.</p>"
    headers = list(rows[0].keys())
    head = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td><pre>{html.escape(json.dumps(value, default=str, ensure_ascii=True, indent=2) if isinstance(value, (dict, list)) else str(value))}</pre></td>" for value in row.values())
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "cdc-lakehouse-ui/1.0"

    def _send_html(self, body: str, status: int = 200) -> None:
        raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_json(self, payload: dict[str, object], status: int = 200) -> None:
        raw = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    @property
    def app(self) -> AppState:
        return self.server.app_state  # type: ignore[attr-defined]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/":
            self._send_html(self._home_page())
            return
        if path == "/state":
            self._send_json(self.app.summary())
            return
        if path == "/healthz":
            self._send_json({"ok": True})
            return
        if path == "/catalog":
            self._send_json(Catalog(None).build(self.app.source, self.app.lake, self.app.warehouse))
            return
        if path == "/changes":
            self._send_html(self._changes_page(parsed.query))
            return
        if path == "/validation-runs":
            rows = self.app.conn.query("SELECT run_id, run_at, status, report_json FROM warehouse.validation_runs ORDER BY run_id DESC").fetchall()
            self._send_html(_render_page("Validation Runs", f"<div class='card'><h2>Validation Runs</h2>{_table_html(rows)}</div>"))
            return
        if path.startswith("/source/"):
            table = path.split("/", 2)[2]
            if table in self.app.source.tracked_tables():
                rows = self.app.conn.query(f"SELECT * FROM source.{table} ORDER BY 1").fetchall()
                self._send_html(_render_page(f"Source {table}", f"<div class='card'><h2>Source: {table}</h2>{_table_html(rows)}</div>"))
                return
        if path.startswith("/warehouse/"):
            parts = path.split("/")
            if len(parts) == 4 and parts[3] in {"current", "history"} and parts[2] in self.app.warehouse.TABLE_DEFS:
                table = parts[2]
                mode = parts[3]
                rows = self.app.conn.query(f"SELECT * FROM warehouse.{table}_{mode} ORDER BY 1").fetchall()
                self._send_html(_render_page(f"Warehouse {table} {mode}", f"<div class='card'><h2>Warehouse: {table} ({mode})</h2>{_table_html(rows)}</div>"))
                return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        data = {key: values[0] for key, values in parse_qs(body).items()}
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        try:
            if path == "/actions/update-customer-status":
                self.app.source.update_customer_status(int(data["customer_id"]), data["customer_status"])
                message = "customer updated in source; run CDC sync to propagate"
            elif path == "/actions/run-sync":
                message = f"synchronized {self.app.pipeline.run_once()} change events"
            elif path == "/actions/run-validate":
                message = json.dumps(self.app.warehouse.validate(), indent=2)
            elif path == "/actions/break-schema":
                self.app.source.introduce_breaking_schema_change()
                message = "introduced incompatible schema drift"
            else:
                self._send_json({"error": "not found"}, status=404)
                return
            self._send_html(_render_page("Action Result", f"<div class='card'><h2>Action Result</h2><pre>{html.escape(message)}</pre><p><a href='/'>Back to overview</a></p></div>"))
        except Exception as exc:  # noqa: BLE001
            self._send_html(_render_page("Error", f"<div class='card'><h2 class='danger'>Action Failed</h2><pre>{html.escape(str(exc))}</pre><p><a href='/'>Back to overview</a></p></div>"), status=500)

    def _home_page(self) -> str:
        summary = self.app.summary()
        pipeline = summary["pipeline_state"]
        actions = f"""
        <div class="card">
          <h2>CDC Lakehouse Overview</h2>
          <p class="muted">Update the source, sync CDC, inspect the lake, and validate the warehouse from this page.</p>
          <div class="row">
            <span class="pill">source</span>
            <span class="pill">lake</span>
            <span class="pill">warehouse</span>
            <span class="pill">catalog</span>
          </div>
          <p><strong>Pipeline status:</strong> <span class="{ 'danger' if pipeline.get('status') == 'blocked' else 'ok' }">{html.escape(str(pipeline.get('status')))}</span></p>
          <p><strong>Checkpoint:</strong> {html.escape(str(pipeline.get('last_change_id')))}</p>
          <p><strong>Source schema hash:</strong> {html.escape(str(pipeline.get('source_schema_hash')))}</p>
        </div>
        <div class="grid" style="margin-top: 16px;">
          <div class="card"><h3>Source counts</h3><pre>{html.escape(json.dumps(summary['source_counts'], indent=2))}</pre></div>
          <div class="card"><h3>Lake count</h3><pre>{summary['lake_count']}</pre></div>
          <div class="card"><h3>Warehouse counts</h3><pre>{html.escape(json.dumps(summary['warehouse_counts'], indent=2))}</pre></div>
          <div class="card"><h3>Drift alerts</h3><pre>{summary['drift_alerts']}</pre></div>
        </div>
        <div class="grid" style="margin-top: 16px;">
          <div class="card">
            <h3>Update Source Customer</h3>
            <form method="post" action="/actions/update-customer-status" class="row">
              <input name="customer_id" placeholder="Customer ID" value="1" />
              <select name="customer_status">
                <option value="active">active</option>
                <option value="suspended">suspended</option>
                <option value="closed">closed</option>
              </select>
              <button type="submit">Update Source</button>
            </form>
          </div>
          <div class="card">
            <h3>CDC / Validation</h3>
            <div class="row">
              <form method="post" action="/actions/run-sync"><button type="submit">Run Sync</button></form>
              <form method="post" action="/actions/run-validate"><button type="submit">Run Validate</button></form>
              <form method="post" action="/actions/break-schema"><button type="submit">Break Schema</button></form>
            </div>
          </div>
        </div>
        <div class="grid" style="margin-top: 16px;">
          <div class="card"><h3>Source tables</h3><p>{' '.join(f"<a href='/source/{t}'>{t}</a>" for t in self.app.source.tracked_tables())}</p></div>
          <div class="card"><h3>Warehouse tables</h3><p>{' '.join(f"<a href='/warehouse/{t}/current'>{t} current</a> | <a href='/warehouse/{t}/history'>{t} history</a>" for t in self.app.warehouse.TABLE_DEFS)}</p></div>
        </div>
        <div class="card" style="margin-top: 16px;">
          <h3>Latest changes</h3>
          <p><a href="/changes">Open the full change log</a></p>
        </div>
        """
        return _render_page("CDC Lakehouse Overview", actions)

    def _changes_page(self, query: str) -> str:
        qs = parse_qs(query)
        table = qs.get("table", [None])[0]
        op = qs.get("op", [None])[0]
        sql = "SELECT change_id, table_name, op, entity_key, before_json, after_json, source_commit_ts, source_txn_id, source_schema_hash, ingested_at FROM lake.cdc_events"
        params: list[object] = []
        clauses: list[str] = []
        if table:
            clauses.append("table_name = ?")
            params.append(table)
        if op:
            clauses.append("op = ?")
            params.append(op)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY change_id DESC"
        rows = self.app.conn.query(sql, params).fetchall()
        filters = f"""
        <form method="get" action="/changes" class="row card">
          <select name="table">
            <option value="">All tables</option>
            {''.join(f"<option value='{html.escape(t)}' {'selected' if table == t else ''}>{html.escape(t)}</option>" for t in self.app.source.tracked_tables())}
          </select>
          <select name="op">
            <option value="">All ops</option>
            <option value="I" {'selected' if op == 'I' else ''}>Insert</option>
            <option value="U" {'selected' if op == 'U' else ''}>Update</option>
            <option value="D" {'selected' if op == 'D' else ''}>Delete</option>
          </select>
          <button type="submit">Filter</button>
        </form>
        """
        return _render_page("All Changes", filters + f"<div class='card' style='margin-top:16px;'><h2>CDC Change Log</h2>{_table_html(rows)}</div>")

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def serve() -> None:
    state = AppState()
    host = os.getenv("CDC_UI_HOST", "0.0.0.0")
    port = int(os.getenv("CDC_UI_PORT", "8080"))
    server = HTTPServer((host, port), RequestHandler)
    server.app_state = state  # type: ignore[attr-defined]
    try:
        server.serve_forever()
    finally:
        state.close()
