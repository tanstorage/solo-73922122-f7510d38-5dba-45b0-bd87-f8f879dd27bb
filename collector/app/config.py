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
    runtime_dir: str        # 进程身份/存活登记与恢复互斥锁

    # ---- 演示开关 ----
    # 打开后，本轮采集在首份 PDF 写入 staging 之后暂停，便于演示：
    # 暂停期间重启另一个 API 进程（其启动恢复不得动这条 running 记录与暂存），
    # 以及强制终止采集进程后由后续启动把它标记 aborted 并回收暂存。
    demo_pause_after_first_pdf: bool
    demo_pause_max_seconds: float
    demo_dir: str
    demo_hold_file: str
    demo_release_file: str


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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
        runtime_dir=os.path.join(data_dir, "run"),
        demo_pause_after_first_pdf=_env_bool("DEMO_PAUSE_AFTER_FIRST_PDF", False),
        demo_pause_max_seconds=float(_env("DEMO_PAUSE_MAX_SECONDS", "20")),
        demo_dir=os.path.join(data_dir, "demo"),
        demo_hold_file=os.path.join(data_dir, "demo", "hold"),
        demo_release_file=os.path.join(data_dir, "demo", "release"),
    )
