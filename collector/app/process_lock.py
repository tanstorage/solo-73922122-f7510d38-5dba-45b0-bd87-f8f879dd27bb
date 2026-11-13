"""单采集进程保护。

两层防护：
1. fcntl 排他文件锁 —— 跨进程、容器重启后由内核自动释放，杜绝多进程并行采集；
2. 采集调度处的线程锁 —— 同进程内并发 HTTP 触发立即得到 409，而不是排队等待。
"""
from __future__ import annotations

import fcntl
import os


class CollectionBusyError(RuntimeError):
    """已有采集正在进行（本进程线程或其它进程持锁）。"""


class ProcessLock:
    def __init__(self, lock_path: str) -> None:
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self._path = lock_path
        self._fh = None  # type: ignore[assignment]

    def acquire_nonblocking(self) -> None:
        fh = open(self._path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            raise CollectionBusyError("另一个采集进程正在运行") from exc
        fh.write(f"pid={os.getpid()}\n")
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

    def __enter__(self) -> "ProcessLock":
        self.acquire_nonblocking()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()
