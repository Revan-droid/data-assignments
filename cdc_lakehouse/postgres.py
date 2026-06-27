from __future__ import annotations

import contextlib
import json
import socket
import struct
from dataclasses import dataclass
from typing import Any, Iterator


class PostgresError(RuntimeError):
    pass


def quote_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return repr(value)
    if isinstance(value, (dict, list)):
        value = json.dumps(value, separators=(",", ":"), sort_keys=True)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def render_sql(sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> str:
    if not params:
        return sql
    pieces = sql.split("?")
    if len(pieces) - 1 != len(params):
        raise PostgresError(f"placeholder count mismatch for sql: {sql}")
    rendered: list[str] = [pieces[0]]
    for piece, param in zip(pieces[1:], params):
        rendered.append(quote_literal(param))
        rendered.append(piece)
    return "".join(rendered)


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    command_tag: str | None = None

    def fetchone(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self.rows)


class PostgresConnection:
    def __init__(self, host: str, port: int, user: str, database: str, password: str | None = None, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self._in_transaction = False
        self._connect()

    def close(self) -> None:
        if self.sock is not None:
            with contextlib.suppress(Exception):
                self._send_message(b"X", b"")
            with contextlib.suppress(Exception):
                self.sock.close()
            self.sock = None

    def _connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        params = [
            b"user",
            self.user.encode("utf-8"),
            b"database",
            self.database.encode("utf-8"),
            b"client_encoding",
            b"UTF8",
            b"application_name",
            b"cdc-lakehouse-app",
        ]
        body = struct.pack("!I", 196608) + b"\x00".join(params) + b"\x00\x00"
        self._send_message(b"", body, startup=True)
        self._read_startup()

    def _send_message(self, code: bytes, payload: bytes, startup: bool = False) -> None:
        if self.sock is None:
            raise PostgresError("connection is closed")
        if startup:
            msg = struct.pack("!I", len(payload) + 4) + payload
        else:
            msg = code + struct.pack("!I", len(payload) + 4) + payload
        self.sock.sendall(msg)

    def _recv_exact(self, n: int) -> bytes:
        if self.sock is None:
            raise PostgresError("connection is closed")
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise PostgresError("unexpected EOF from postgres")
            data += chunk
        return data

    def _recv_message(self) -> tuple[bytes, bytes]:
        code = self._recv_exact(1)
        length = struct.unpack("!I", self._recv_exact(4))[0]
        payload = self._recv_exact(length - 4)
        return code, payload

    def _read_cstring(self, payload: bytes, idx: int) -> tuple[str, int]:
        end = payload.index(b"\x00", idx)
        return payload[idx:end].decode("utf-8"), end + 1

    def _read_startup(self) -> None:
        while True:
            code, payload = self._recv_message()
            if code == b"R":
                auth_type = struct.unpack("!I", payload[:4])[0]
                if auth_type == 0:
                    continue
                if auth_type == 3:
                    if self.password is None:
                        raise PostgresError("postgres requested cleartext password but none was provided")
                    self._send_message(b"p", self.password.encode("utf-8") + b"\x00")
                    continue
                raise PostgresError(f"unsupported postgres auth method: {auth_type}")
            if code == b"S":
                continue
            if code == b"K":
                continue
            if code == b"N":
                continue
            if code == b"E":
                raise PostgresError(self._parse_error(payload))
            if code == b"Z":
                return

    def _parse_error(self, payload: bytes) -> str:
        idx = 0
        parts = {}
        while idx < len(payload) and payload[idx] != 0:
            field_type = chr(payload[idx])
            idx += 1
            field_value, idx = self._read_cstring(payload, idx)
            parts[field_type] = field_value
        return parts.get("M", "postgres error")

    @staticmethod
    def _coerce_value(value: str, type_oid: int) -> Any:
        if type_oid in {20, 21, 23}:
            return int(value)
        if type_oid in {700, 701, 1700}:
            return float(value)
        if type_oid == 16:
            return value == "t"
        if type_oid in {114, 3802}:
            return json.loads(value)
        return value

    def _parse_data_row(self, payload: bytes, columns: list[tuple[str, int]]) -> dict[str, Any]:
        count = struct.unpack("!H", payload[:2])[0]
        idx = 2
        row: dict[str, Any] = {}
        for i in range(count):
            length = struct.unpack("!i", payload[idx:idx + 4])[0]
            idx += 4
            if length == -1:
                row[columns[i][0]] = None
                continue
            value = payload[idx:idx + length].decode("utf-8")
            idx += length
            row[columns[i][0]] = self._coerce_value(value, columns[i][1])
        return row

    def _consume_query_result(self) -> QueryResult:
        columns: list[tuple[str, int]] = []
        column_names: list[str] = []
        rows: list[dict[str, Any]] = []
        command_tag: str | None = None
        while True:
            code, payload = self._recv_message()
            if code == b"T":
                field_count = struct.unpack("!H", payload[:2])[0]
                idx = 2
                columns = []
                for _ in range(field_count):
                    name, idx = self._read_cstring(payload, idx)
                    idx += 4
                    idx += 2
                    type_oid = struct.unpack("!I", payload[idx:idx + 4])[0]
                    idx += 4
                    idx += 8
                    columns.append((name, type_oid))
                    column_names.append(name)
            elif code == b"D":
                rows.append(self._parse_data_row(payload, columns))
            elif code == b"C":
                command_tag = payload[:-1].decode("utf-8")
            elif code == b"E":
                raise PostgresError(self._parse_error(payload))
            elif code == b"Z":
                return QueryResult(columns=column_names, rows=rows, command_tag=command_tag)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> str | None:
        result = self.query(sql, params)
        return result.command_tag

    def query(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> QueryResult:
        if self.sock is None:
            raise PostgresError("connection is closed")
        rendered = render_sql(sql, params)
        self._send_message(b"Q", rendered.encode("utf-8") + b"\x00")
        return self._consume_query_result()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        if self._in_transaction:
            raise PostgresError("nested transactions are not supported")
        self._in_transaction = True
        try:
            self.execute("BEGIN")
            yield
            self.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                self.execute("ROLLBACK")
            raise
        finally:
            self._in_transaction = False

    def scalar(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None, default: Any = None) -> Any:
        row = self.query(sql, params).fetchone()
        if row is None:
            return default
        return next(iter(row.values()))
