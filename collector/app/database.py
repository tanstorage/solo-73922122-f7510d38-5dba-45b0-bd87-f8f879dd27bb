"""SQLite 持久层。

记录 UIDVALIDITY、UID、逐封采集进度以及附件索引。
所有"索引 + 进度"的写入都在同一事务内提交（见 collector.pipeline），
查询接口只可能看到完整提交的邮件，永远不会看到半封附件。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS mailbox_epochs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    uidvalidity     INTEGER NOT NULL UNIQUE,
    mailbox         TEXT NOT NULL,
    observed_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id        INTEGER NOT NULL REFERENCES mailbox_epochs(id),
    uid             INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('done', 'failed', 'oversize')),
    failure_reason  TEXT,
    message_size    INTEGER,
    subject         TEXT,
    from_header     TEXT,
    message_id_hdr  TEXT,
    collected_at    TEXT,
    UNIQUE(epoch_id, uid)
);

CREATE TABLE IF NOT EXISTS attachments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      INTEGER NOT NULL REFERENCES messages(id),
    part_number     TEXT NOT NULL,
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    size_bytes      INTEGER NOT NULL,
    storage_path    TEXT NOT NULL,
    stored_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(message_id, part_number)
);

CREATE TABLE IF NOT EXISTS collection_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id            INTEGER REFERENCES mailbox_epochs(id),
    uidvalidity         INTEGER,
    started_at          TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'aborted', 'busy')),
    total_seen          INTEGER NOT NULL DEFAULT 0,
    processed_ok        INTEGER NOT NULL DEFAULT 0,
    failed              INTEGER NOT NULL DEFAULT 0,
    oversize            INTEGER NOT NULL DEFAULT 0,
    skipped_terminal    INTEGER NOT NULL DEFAULT 0,
    attachments_saved   INTEGER NOT NULL DEFAULT 0,
    note                TEXT
);
"""


class Database:
    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
            # 上次进程退出时仍处于 running 的采集，标记为 aborted（可被重新采集）
            self._conn.execute(
                "UPDATE collection_runs SET status='aborted', "
                "finished_at=datetime('now'), "
                "note=COALESCE(note,'') || ' [进程重启，自动中断标记]' "
                "WHERE status='running'"
            )
            self._conn.commit()

    # ---- 基础工具 ----
    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def query_all(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchone()

    def begin(self) -> sqlite3.Connection:
        """调用方在同一线程内随后使用 commit()/rollback()。"""
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def commit(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._lock.release()

    def rollback(self) -> None:
        try:
            self._conn.rollback()
        finally:
            self._lock.release()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- UIDVALIDITY / 邮箱代际 ----
    def get_or_create_epoch(self, uidvalidity: int, mailbox: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM mailbox_epochs WHERE uidvalidity=?",
                (uidvalidity,),
            ).fetchone()
            if row is not None:
                return int(row["id"])
            cur = self._conn.execute(
                "INSERT INTO mailbox_epochs(uidvalidity, mailbox) VALUES (?, ?)",
                (uidvalidity, mailbox),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def latest_epoch(self) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT * FROM mailbox_epochs ORDER BY uidvalidity DESC, id DESC LIMIT 1"
        )

    def list_epochs(self) -> list[sqlite3.Row]:
        return self.query_all("SELECT * FROM mailbox_epochs ORDER BY id")

    # ---- 采集运行记录 ----
    def start_run(self, uidvalidity: Optional[int], epoch_id: Optional[int]) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO collection_runs(epoch_id, uidvalidity) VALUES (?, ?)",
                (epoch_id, uidvalidity),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, counters: dict[str, int],
                   note: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE collection_runs SET status=?, finished_at=datetime('now'), "
                "total_seen=?, processed_ok=?, failed=?, oversize=?, "
                "skipped_terminal=?, attachments_saved=?, note=? WHERE id=?",
                (
                    status,
                    counters.get("total_seen", 0),
                    counters.get("processed_ok", 0),
                    counters.get("failed", 0),
                    counters.get("oversize", 0),
                    counters.get("skipped_terminal", 0),
                    counters.get("attachments_saved", 0),
                    note,
                    run_id,
                ),
            )
            self._conn.commit()

    def list_runs(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.query_all(
            "SELECT * FROM collection_runs ORDER BY id DESC LIMIT ?", (limit,)
        )

    def attach_epoch(self, run_id: int, epoch_id: int, uidvalidity: int) -> None:
        """IMAP 连接成功、拿到 UIDVALIDITY 后，把运行记录挂到对应邮箱代际。"""
        with self._lock:
            self._conn.execute(
                "UPDATE collection_runs SET epoch_id=?, uidvalidity=? WHERE id=?",
                (epoch_id, uidvalidity, run_id),
            )
            self._conn.commit()

    # ---- 单封邮件：进度 + 索引在同一事务内提交 ----
    def get_message(self, epoch_id: int, uid: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT * FROM messages WHERE epoch_id=? AND uid=?", (epoch_id, uid)
        )

    def insert_message(self, conn: sqlite3.Connection, epoch_id: int, uid: int,
                       status: str, failure_reason: Optional[str],
                       message_size: Optional[int], subject: Optional[str],
                       from_header: Optional[str], message_id_hdr: Optional[str]) -> int:
        cur = conn.execute(
            "INSERT INTO messages(epoch_id, uid, status, failure_reason, message_size, "
            "subject, from_header, message_id_hdr, collected_at) "
            "VALUES (?,?,?,?,?,?,?,?, datetime('now'))",
            (epoch_id, uid, status, failure_reason, message_size,
             subject, from_header, message_id_hdr),
        )
        return int(cur.lastrowid)

    def insert_attachment(self, conn: sqlite3.Connection, message_row_id: int,
                          part_number: str, filename: str, content_type: str,
                          size_bytes: int, storage_path: str) -> None:
        conn.execute(
            "INSERT INTO attachments(message_id, part_number, filename, content_type, "
            "size_bytes, storage_path) VALUES (?,?,?,?,?,?)",
            (message_row_id, part_number, filename, content_type,
             size_bytes, storage_path),
        )

    # ---- 查询接口 ----
    def list_messages(self, epoch_id: Optional[int] = None,
                      status: Optional[str] = None, limit: int = 200) -> list[sqlite3.Row]:
        sql = (
            "SELECT m.*, e.uidvalidity FROM messages m "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE 1=1"
        )
        params: list[Any] = []
        if epoch_id is not None:
            sql += " AND m.epoch_id=?"
            params.append(epoch_id)
        if status is not None:
            sql += " AND m.status=?"
            params.append(status)
        sql += " ORDER BY e.uidvalidity, m.uid LIMIT ?"
        params.append(limit)
        return self.query_all(sql, params)

    def get_message_with_epoch(self, message_pk: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT m.*, e.uidvalidity FROM messages m "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE m.id=?",
            (message_pk,),
        )

    def list_attachments(self, message_pk: int) -> list[sqlite3.Row]:
        return self.query_all(
            "SELECT * FROM attachments WHERE message_id=? ORDER BY part_number",
            (message_pk,),
        )

    def get_attachment(self, attachment_pk: int) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT a.*, m.epoch_id, m.uid, e.uidvalidity FROM attachments a "
            "JOIN messages m ON m.id=a.message_id "
            "JOIN mailbox_epochs e ON e.id=m.epoch_id WHERE a.id=?",
            (attachment_pk,),
        )

    def progress_for_epoch(self, epoch_id: int) -> sqlite3.Row:
        return self.query_one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed, "
            "SUM(CASE WHEN status='oversize' THEN 1 ELSE 0 END) AS oversize, "
            "COALESCE((SELECT COUNT(*) FROM attachments a JOIN messages m2 "
            "          ON m2.id=a.message_id WHERE m2.epoch_id=?),0) AS attachments "
            "FROM messages WHERE epoch_id=?",
            (epoch_id, epoch_id),
        )
