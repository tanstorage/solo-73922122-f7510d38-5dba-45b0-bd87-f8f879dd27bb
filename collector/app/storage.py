"""原件落盘：先写 staging 临时目录，整封附件全部完成后再原子发布。

目录布局（相对 blob_root）：
    uidvalidity-<UIDVALIDITY>/uid-<UID>/<部分序号>__<文件名>

- 半封附件只存在于 staging，API 只索引 blob_root 下的正式文件，因此断流/落盘
  失败/进程退出都不会暴露半封附件；
- 正式目录使用 os.rename 原子发布（同文件系统内 rename 为原子操作）；
- 路径由 UIDVALIDITY、UID、MIME 部分序号决定，重试天然幂等，不会重复或覆盖错文件；
- 暂存目录名携带运行 ID，进程崩溃后的回收只针对已死亡运行，不会误删新任务。
"""
from __future__ import annotations

import os
import shutil
import uuid
from typing import Sequence

from .pdf_extract import PdfPart, stored_filename


class Storage:
    def __init__(self, blob_root: str, staging_root: str) -> None:
        self.blob_root = blob_root
        self.staging_root = staging_root
        os.makedirs(blob_root, exist_ok=True)
        os.makedirs(staging_root, exist_ok=True)

    def relative_dir(self, uidvalidity: int, uid: int) -> str:
        return os.path.join(f"uidvalidity-{uidvalidity}", f"uid-{uid}")

    def absolute_path(self, relative_path: str) -> str:
        return os.path.join(self.blob_root, relative_path)

    def new_staging_dir(self, run_id: int) -> str:
        """创建本运行的暂存目录。

        目录名带上运行 ID（``tmp-run-<run_id>-<uuid>``），崩溃恢复据此可以
        精确判断归属：只回收"已不在运行"的运行名下目录，绝不会碰到仍在
        采集的新任务目录。
        """
        path = os.path.join(
            self.staging_root, f"tmp-run-{run_id}-{uuid.uuid4().hex}"
        )
        os.makedirs(path, exist_ok=False)
        return path

    def write_staged(self, staging_dir: str, part: PdfPart) -> tuple[str, int]:
        """把单个 PDF 写入 staging，返回 (落盘文件名, 字节数)。

        采用 临时文件 + fsync + rename，写入中途崩溃也只留下隐藏的临时文件。
        完整的正式相对路径在 publish_all 中统一生成。
        """
        tmp_name = f".{stored_filename(part)}.part"
        tmp_path = os.path.join(staging_dir, tmp_name)
        with open(tmp_path, "wb") as fh:
            fh.write(part.payload)
            fh.flush()
            os.fsync(fh.fileno())
        final_in_staging = os.path.join(staging_dir, stored_filename(part))
        os.rename(tmp_path, final_in_staging)
        return stored_filename(part), len(part.payload)

    def publish_all(self, staging_dir: str, parts: Sequence[PdfPart],
                    uidvalidity: int, uid: int) -> list[tuple[str, int]]:
        """整封附件全部落盘成功后，一次性原子发布到正式目录。

        返回 [(storage 相对路径, 字节数), ...]，顺序与 parts 一致。
        """
        rel_dir = os.path.join(self.blob_root, self.relative_dir(uidvalidity, uid))
        os.makedirs(rel_dir, exist_ok=True)
        published: list[tuple[str, int]] = []
        for part in parts:
            name = stored_filename(part)
            src = os.path.join(staging_dir, name)
            dst = os.path.join(rel_dir, name)
            # 重试场景：同 UID + 同部分序号的旧正式文件可被安全替换
            os.replace(src, dst)
            published.append(
                (os.path.join(self.relative_dir(uidvalidity, uid), name),
                 os.path.getsize(dst))
            )
        # 目录项落盘，尽量保证崩溃后文件可见
        self._fsync_dir(rel_dir)
        return published

    def discard(self, staging_dir: str) -> None:
        """失败/断流后清理该封的全部临时文件。"""
        shutil.rmtree(staging_dir, ignore_errors=True)

    # ---- 崩溃恢复：只删明确归属，绝不按前缀误伤新任务 ----
    def _is_within_staging_root(self, path: str) -> bool:
        root = os.path.realpath(self.staging_root)
        target = os.path.realpath(path)
        try:
            return os.path.commonpath([root, target]) == root and target != root
        except ValueError:
            return False

    def remove_staging(self, staging_dir: str) -> bool:
        """回收某个运行自己登记的 staging 目录（成功/失败均幂等）。"""
        if not staging_dir or not os.path.isdir(staging_dir):
            return False
        if not self._is_within_staging_root(staging_dir):
            return False
        shutil.rmtree(staging_dir, ignore_errors=True)
        return True

    def reap_staging(self, dead_run_ids: set[int],
                     run_status_lookup, grace_seconds: float = 3600) -> list[str]:
        """按运行归属回收 staging 目录。

        规则（保守优先，宁可不删也不误伤）：
        - ``tmp-run-<id>-*`` 且 id 属于本次判定死亡的运行 → 删除；
        - ``tmp-run-<id>-*`` 且 id 对应数据库中 nonexistent 的运行 → 保留
          （进程可能刚 start_run、尚未 create 目录/登记，属于并发窗口）；
        - ``tmp-run-<id>-*`` 且 id 对应数据库中已结束（非 running）的运行 →
          删除（崩溃在 create 与登记之间的孤儿）；
        - ``tmp-run-<id>-*`` 且 id 对应数据库中的 running 运行 → 保留；
        - 非 ``tmp-run-`` 前缀的历史目录 → 仅在超过宽限期后删除。
        """
        import re
        import time

        removed: list[str] = []
        if not os.path.isdir(self.staging_root):
            return removed
        now = time.time()
        pattern = re.compile(r"^tmp-run-(\d+)-")

        for name in os.listdir(self.staging_root):
            path = os.path.join(self.staging_root, name)
            if not os.path.isdir(path):
                continue
            match = pattern.match(name)
            if match:
                run_id = int(match.group(1))
                if run_id in dead_run_ids:
                    should_remove = True
                else:
                    status = run_status_lookup(run_id)
                    should_remove = status is not None and status != "running"
            else:
                # 旧命名 / 无法判定归属：只清明显陈旧的
                try:
                    should_remove = (now - os.path.getmtime(path)) >= grace_seconds
                except OSError:
                    should_remove = False
            if should_remove and self.remove_staging(path):
                removed.append(path)
        return removed

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass
