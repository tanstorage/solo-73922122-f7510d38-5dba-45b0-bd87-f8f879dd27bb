"""原件落盘：先写 staging 临时目录，整封附件全部完成后再原子发布。

目录布局（相对 blob_root）：
    uidvalidity-<UIDVALIDITY>/uid-<UID>/<部分序号>__<文件名>

- 半封附件只存在于 staging，API 只索引 blob_root 下的正式文件，因此断流/落盘
  失败/进程退出都不会暴露半封附件；
- 正式目录使用 os.rename 原子发布（同文件系统内 rename 为原子操作）；
- 路径由 UIDVALIDITY、UID、MIME 部分序号决定，重试天然幂等，不会重复或覆盖错文件。
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

    def new_staging_dir(self) -> str:
        path = os.path.join(self.staging_root, f"tmp-{uuid.uuid4().hex}")
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

    def cleanup_stale_staging(self) -> int:
        """进程启动时清掉上次崩溃遗留的临时目录，返回清理数量。"""
        removed = 0
        if not os.path.isdir(self.staging_root):
            return removed
        for name in os.listdir(self.staging_root):
            if name.startswith("tmp-"):
                shutil.rmtree(os.path.join(self.staging_root, name), ignore_errors=True)
                removed += 1
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
