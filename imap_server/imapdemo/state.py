"""本地演示 IMAP 服务的邮箱状态。

状态文件 state.json（加文件锁读写），邮件原文存为 <uid>.eml。
支持三类演示操作：
- delete_before(n)：删除最早 n 封 —— UID 保持不变、序号前移，演示 UID 取信不受序号影响；
- rebuild()：邮箱重建，生成新 UIDVALIDITY，UID 从新空间重新分配；
- set_flaky(uid)：设置断流 UID，FETCH 该邮件正文时发送一半字节后主动断开连接。
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class MailMessage:
    uid: int
    eml_path: str
    size: int
    internaldate: str
    flags: tuple[str, ...] = ()


class MailState:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.state_path = os.path.join(self.data_dir, "state.json")
        self._lock_path = os.path.join(self.data_dir, ".state.lock")
        if not os.path.exists(self.state_path):
            self._write_locked({
                "uidvalidity": int(time.time()) & 0x7FFFFFFF,
                "next_uid": 1,
                "flaky_uid": None,
                "messages": [],
            })

    # ---- 底层加锁读写 ----
    def _read_raw(self) -> dict:
        with open(self.state_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write_locked(self, payload: dict) -> None:
        os.makedirs(os.path.dirname(self._lock_path), exist_ok=True)
        with open(self._lock_path, "a+") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            tmp = self.state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.state_path)

    def snapshot(self) -> dict:
        """读取当前状态快照（服务端每条连接/命令前调用，立即感知 CLI 的改动）。"""
        return self._read_raw()

    def update(self, mutator) -> dict:
        """在排他锁内 读取-修改-写回，mutator(payload)->None。返回新快照。"""
        with open(self._lock_path, "a+") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            payload = self._read_raw()
            mutator(payload)
            tmp = self.state_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.state_path)
            return payload

    # ---- 便捷访问 ----
    @property
    def uidvalidity(self) -> int:
        return int(self.snapshot()["uidvalidity"])

    def get_message(self, uid: int) -> Optional[dict]:
        for msg in self.snapshot()["messages"]:
            if int(msg["uid"]) == uid:
                return msg
        return None

    # ---- 演示操作 ----
    def append_message(self, eml: bytes, internaldate: str,
                       flags: tuple[str, ...] = ()) -> dict:
        def mutate(payload: dict) -> None:
            uid = int(payload["next_uid"])
            eml_name = f"uv{int(payload['uidvalidity'])}-{uid}.eml"
            eml_path = os.path.join(self.data_dir, eml_name)
            with open(eml_path, "wb") as fh:
                fh.write(eml)
                fh.flush()
                os.fsync(fh.fileno())
            payload["messages"].append({
                "uid": uid,
                "eml_path": eml_path,
                "size": len(eml),
                "internaldate": internaldate,
                "flags": list(flags),
            })
            payload["next_uid"] = uid + 1
        return self.update(mutate)

    def delete_before(self, count: int) -> dict:
        """删除当前序列中最早的 count 封（同时删除 eml 文件）。"""
        def mutate(payload: dict) -> None:
            victims = payload["messages"][:count]
            for msg in victims:
                try:
                    os.remove(msg["eml_path"])
                except OSError:
                    pass
            payload["messages"] = payload["messages"][count:]
            if payload.get("flaky_uid") and not any(
                m["uid"] == payload["flaky_uid"] for m in payload["messages"]
            ):
                payload["flaky_uid"] = None
        return self.update(mutate)

    def rebuild(self) -> dict:
        """邮箱重建：新 UIDVALIDITY、新 next_uid、空消息列表、清除断流标记。

        旧 eml 文件与旧状态产生的采集原件均保留，仅本邮箱视图切换到新 UID 空间。
        """
        def mutate(payload: dict) -> None:
            payload["uidvalidity"] = (int(time.time()) * 1000 + os.getpid()) & 0x7FFFFFFF
            payload["next_uid"] = 1
            payload["messages"] = []
            payload["flaky_uid"] = None
        return self.update(mutate)

    def set_flaky(self, uid: Optional[int]) -> dict:
        def mutate(payload: dict) -> None:
            if uid is not None and not any(
                int(m["uid"]) == uid for m in payload["messages"]
            ):
                raise KeyError(f"UID {uid} 不存在")
            payload["flaky_uid"] = uid
        return self.update(mutate)

    def read_eml(self, msg: dict) -> bytes:
        with open(msg["eml_path"], "rb") as fh:
            return fh.read()
