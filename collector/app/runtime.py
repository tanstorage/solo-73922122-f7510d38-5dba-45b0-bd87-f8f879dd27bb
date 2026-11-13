"""跨进程运行身份与存活判定。

两个 API 进程共享同一个 SQLite 与原件目录，因此"谁还活着"不能靠 PID
（不同容器/PID 命名空间里 PID 会重复，且无法互相 kill(0) 探测）。

做法：每个 API 进程启动时在共享的 ``<DATA_DIR>/run/processes/`` 下创建
一个唯一的登记文件，并在自己的整个生命周期内持有该文件的 ``flock`` 排他锁。
内核会在进程退出（包括 SIGKILL）时自动释放锁，所以：

- 能对同一文件重新拿到排他锁 ⇒ 原持有者已经死亡；
- 拿不到锁（EWOULDBLOCK）⇒ 原持有者仍然活着。

由于锁作用在共享卷上的同一 inode，该判定在两个容器间同样有效。
"""
from __future__ import annotations

import fcntl
import json
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ProcessRegistry:
    """登记本进程身份，并判定其它进程登记是否仍存活。"""

    def __init__(self, runtime_dir: str) -> None:
        self.runtime_dir = runtime_dir
        self.registry_dir = os.path.join(runtime_dir, "processes")
        os.makedirs(self.registry_dir, exist_ok=True)
        self._fh = None  # type: ignore[assignment]
        self._path: Optional[str] = None
        self.token: Optional[str] = None

    # ---- 本进程登记 ----
    def register(self) -> str:
        token = uuid.uuid4().hex
        path = self._path_for(token)
        fh = open(path, "a+")
        # 文件名本身唯一，排他锁必然成功；锁的意义在于"活着"这一事实本身
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        payload = {
            "token": token,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "started_at": _now(),
            "version": 1,
        }
        fh.seek(0)
        json.dump(payload, fh)
        fh.flush()
        os.fsync(fh.fileno())
        self._fh, self._path, self.token = fh, path, token
        return token

    def unregister(self) -> None:
        """正常退出：释放锁并删除登记文件。异常退出时文件由新进程回收。"""
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None
        if self._path is not None:
            try:
                os.unlink(self._path)
            except FileNotFoundError:
                pass
            self._path = None

    def close(self) -> None:
        """测试/异常路径：仅释放锁，不删登记文件（模拟进程直接消失）。"""
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None

    # ---- 其它进程存活判定 ----
    def _path_for(self, token: str) -> str:
        return os.path.join(self.registry_dir, f"proc-{token}.lock")

    def alive(self, token: Optional[str]) -> bool:
        """token 对应的进程是否仍存活。

        能拿到排他锁说明原持有者已消失；顺手回收其登记文件。
        """
        if not token:
            return False
        path = self._path_for(token)
        if not os.path.exists(path):
            return False
        fh = open(path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return True
        # 拿到锁 ⇒ 没有活着的持有者
        fh.close()
        try:
            os.unlink(path)
        except OSError:
            pass
        return False

    def describe(self, token: str) -> dict[str, Any]:
        path = self._path_for(token)
        alive = self.alive(token)
        info: dict[str, Any] = {"token": token, "alive": alive}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    info.update(json.load(fh))
            except (OSError, ValueError):
                pass
        info["alive"] = alive
        return info

    def describe_all(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for name in sorted(os.listdir(self.registry_dir)):
            if not (name.startswith("proc-") and name.endswith(".lock")):
                continue
            token = name[len("proc-"):-len(".lock")]
            info = self.describe(token)
            if info.get("alive") is not None:
                items.append(info)
        return items
