"""采集服务配置：IMAP 连接信息、邮箱名及存储路径全部由环境变量固定。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # ---- IMAP（地址、账号、邮箱名由配置固定）----
    imap_host: str
    imap_port: int
    imap_user: str
    imap_password: str
    imap_mailbox: str
    socket_timeout: int

    # ---- 业务限制 ----
    max_message_bytes: int  # 每封邮件上限，默认 8 MiB

    # ---- 本地存储 ----
    data_dir: str           # SQLite 与原件根目录
    db_path: str
    blob_root: str          # PDF 原件根目录
    staging_dir: str        # 半封附件的临时落盘区（对外不可见）
    lock_path: str          # 单采集进程文件锁


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def load_settings() -> Settings:
    data_dir = _env("DATA_DIR", "/data")
    return Settings(
        imap_host=_env("IMAP_HOST", "imap"),
        imap_port=int(_env("IMAP_PORT", "143")),
        imap_user=_env("IMAP_USER", "invoicing@example.com"),
        imap_password=_env("IMAP_PASSWORD", "invoice-pass"),
        imap_mailbox=_env("IMAP_MAILBOX", "INBOX"),
        socket_timeout=int(_env("IMAP_SOCKET_TIMEOUT", "30")),
        max_message_bytes=int(_env("MAX_MESSAGE_BYTES", str(8 * 1024 * 1024))),
        data_dir=data_dir,
        db_path=os.path.join(data_dir, "collector.db"),
        blob_root=os.path.join(data_dir, "blobs"),
        staging_dir=os.path.join(data_dir, "staging"),
        lock_path=os.path.join(data_dir, "collect.lock"),
    )
