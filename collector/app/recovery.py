"""启动恢复：只回收"死主"的运行，绝不动活着的进程。

两个 API 进程共库共盘，启动时的恢复必须满足：

1. 另一进程仍在采集（其运行记录为 running、其进程登记锁仍被持有）时，
   保留该运行记录与其 staging 暂存，**不标 aborted、不删文件**；
2. 采集进程被 SIGKILL / 容器被强制终止后，其进程登记锁由内核自动释放，
   后续启动据此判定"死主"，把该运行标 aborted 并只回收它自己登记的
   staging 目录，从而允许重新触发续采；
3. 恢复与新采集、两个进程同时启动相遇时：
   - 恢复在独立的 recovery 文件锁内串行执行；
   - 进程登记（register）在数据库恢复之前完成，新运行在插入时即带上
     owner_token 与 staging_dir，故不会被误判；
   - 只删除死主运行记录里明确的 staging 目录，绝不按前缀清空整个目录，
     因此不会清掉新任务文件、不会撤销已完成运行、不会碰 blobs 正式附件。
"""
from __future__ import annotations

import fcntl
import os
from typing import Any, Optional

from .database import Database
from .runtime import ProcessRegistry
from .storage import Storage


class Recovery:
    def __init__(self, db: Database, storage: Storage,
                 registry: ProcessRegistry, runtime_dir: str,
                 collect_lock_path: str) -> None:
        self._db = db
        self._storage = storage
        self._registry = registry
        self._lock_path = os.path.join(runtime_dir, "recovery.lock")
        self._collect_lock_path = collect_lock_path

    # ------------------------------------------------------------------
    def run(self) -> dict[str, Any]:
        """执行一次启动恢复。返回本次处理的摘要（供 /health、日志使用）。"""
        os.makedirs(os.path.dirname(self._lock_path), exist_ok=True)
        with open(self._lock_path, "a+") as lock_fh:
            # 串行化：两个进程同时启动时只有一个在做恢复，另一个等它做完
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                return self._recover()
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    # ------------------------------------------------------------------
    def _recover(self) -> dict[str, Any]:
        running = self._db.query_all(
            "SELECT * FROM collection_runs WHERE status='running' ORDER BY id"
        )
        aborted: list[int] = []
        preserved: list[int] = []
        dead_run_ids: set[int] = set()

        for run in running:
            owner = run["owner_token"]
            if self._owner_alive(owner, run):
                preserved.append(int(run["id"]))
                continue
            # 死主：标记 aborted（只动未完成运行，已完成的一律不碰）
            if not self._db.abort_run(
                int(run["id"]),
                " [进程被强制终止，启动恢复：标记 aborted]"
                if owner else " [历史运行缺少进程登记，启动恢复：标记 aborted]",
            ):
                # 已被并发恢复处理，保持原样
                continue
            aborted.append(int(run["id"]))
            dead_run_ids.add(int(run["id"]))

        # 按运行归属回收暂存：只删已死亡运行的目录；仍 running 的（包括另一
        # 进程刚插入、尚未登记目录的新运行）一律保留。
        removed_staging = self._storage.reap_staging(
            dead_run_ids,
            run_status_lookup=self._db.run_status,
        )

        return {
            "aborted_runs": aborted,
            "preserved_runs": preserved,
            "preserved_staging": self._preserved_staging_dirs(preserved),
            "removed_staging": removed_staging,
        }

    def _preserved_staging_dirs(self, run_ids: list[int]) -> list[str]:
        dirs: list[str] = []
        for run_id in run_ids:
            row = self._db.get_run(run_id)
            if row is not None and row["staging_dir"]:
                dirs.append(row["staging_dir"])
        return dirs

    # ------------------------------------------------------------------
    def _owner_alive(self, owner: Optional[str], run) -> bool:
        if owner:
            return self._registry.alive(owner)
        # 旧版数据没有 owner_token：无法按进程身份判定。若采集文件锁此刻被持有，
        # 说明可能仍有一轮采集在进行，保守保留；否则视为死主。
        return not self._collect_lock_is_free()

    def _collect_lock_is_free(self) -> bool:
        path = self._collect_lock_path
        if not os.path.exists(path):
            return True
        try:
            with open(path, "a+") as fh:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    return False
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                return True
        except OSError:
            return False
