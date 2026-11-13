"""采集流水线：逐封 UID 拉取、附件原子落盘、索引与进度同事务提交。

故障语义（对应需求）：
- 断流 / 落盘失败 / 进程退出：该封不推进进度、staging 临时件被丢弃（或重启清理），
  正式目录与数据库均无该封的任何痕迹，API 不可能看到半封附件；
- 重启续采：已落库邮件（done/oversize/failed）按 (UIDVALIDITY, UID) 跳过，
  断流/落盘失败等未落库的邮件重新拉取；正式路径由 UIDVALIDITY/UID/部分序号确定，重试幂等、不重复；
- 邮箱重建（UIDVALIDITY 变化）：建立新的邮箱代际记录，在新 UID 空间重新采集，
  旧代际的原件与来源记录原样保留、仍可查询下载。
"""
from __future__ import annotations

import threading
from typing import Any, Optional

from .config import Settings
from .database import Database
from .imap_client import ImapError, ImapSession
from .pdf_extract import iter_pdf_parts, stored_filename, summarize_headers
from .process_lock import CollectionBusyError, ProcessLock
from .storage import Storage

# 终态：凡已落库的邮件（done/oversize/failed）下轮一律跳过。
# - done/oversize：成功或确定性超限，不重复采集；
# - failed：同一 UID 即同一封邮件、字节不变，解析失败可确定性复现，重试无意义，
#   且跳过可避免再次 INSERT 撞 UNIQUE(epoch_id, uid) 约束。
# 断流/落盘失败的邮件根本不会落库，因此仍会在后续轮次重新采集。
TERMINAL_STATUSES = {"done", "oversize", "failed"}


class Collector:
    def __init__(self, settings: Settings, db: Database, storage: Storage) -> None:
        self._settings = settings
        self._db = db
        self._storage = storage
        self._process_lock = ProcessLock(settings.lock_path)
        # 同进程并发触发保护：第二个请求立即失败，不排队
        self._thread_lock = threading.Lock()

    def run_collection(self) -> dict[str, Any]:
        if not self._thread_lock.acquire(blocking=False):
            raise CollectionBusyError("本进程已有采集在执行")
        try:
            # 跨进程排他锁：容器内若有第二个采集进程，拿不到锁
            with self._process_lock:
                return self._run_locked()
        finally:
            self._thread_lock.release()

    # ------------------------------------------------------------------
    def _run_locked(self) -> dict[str, Any]:
        s = self._settings
        db = self._db
        storage = self._storage
        run_id = db.start_run(uidvalidity=None, epoch_id=None)
        counters = {
            "total_seen": 0, "processed_ok": 0, "failed": 0,
            "oversize": 0, "skipped_terminal": 0, "attachments_saved": 0,
        }
        abort_reason: Optional[str] = None

        try:
            with ImapSession(
                s.imap_host, s.imap_port, s.imap_user, s.imap_password,
                s.imap_mailbox, s.socket_timeout,
            ) as session:
                assert session.uidvalidity is not None
                uidvalidity = session.uidvalidity
                epoch_id = db.get_or_create_epoch(uidvalidity, s.imap_mailbox)
                db.attach_epoch(run_id, epoch_id, uidvalidity)

                uids = session.search_all_uids()
                for uid in uids:
                    counters["total_seen"] += 1
                    existing = db.get_message(epoch_id, uid)
                    if existing is not None and existing["status"] in TERMINAL_STATUSES:
                        counters["skipped_terminal"] += 1
                        continue

                    outcome = self._process_one(
                        session, storage, uidvalidity, epoch_id, uid, counters
                    )
                    if outcome is not None:
                        # IMAP/磁盘级故障：中止本轮，未处理的邮件下轮续采
                        abort_reason = outcome
                        break
        except ImapError as exc:
            abort_reason = str(exc)
        except Exception as exc:  # 防御性：任何意外都不让 run 卡在 running
            abort_reason = f"采集异常中止: {exc!r}"

        status = "aborted" if abort_reason else "completed"
        db.finish_run(run_id, status, counters, note=abort_reason)
        return {
            "run_id": run_id,
            "status": status,
            "abort_reason": abort_reason,
            "counters": counters,
        }

    # ------------------------------------------------------------------
    def _process_one(self, session: ImapSession, storage: Storage,
                     uidvalidity: int, epoch_id: int, uid: int,
                     counters: dict[str, int]) -> Optional[str]:
        """处理单封邮件。返回 None 表示成功/可继续；返回字符串表示中止原因。"""
        db = self._db
        limit = self._settings.max_message_bytes
        staging = storage.new_staging_dir()
        try:
            # 1) 先查声明大小，超限不拉正文（第一道 8MiB 校验）
            declared_size = session.fetch_size(uid)
            if declared_size is not None and declared_size > limit:
                self._commit_oversize(epoch_id, uid, declared_size)
                counters["oversize"] += 1
                return None

            # 2) BODY.PEEK[] 拉完整原文（不改变已读状态）；断流会抛 ImapError
            raw = session.fetch_raw(uid)

            # 3) 实际字节再校验一次（第二道 8MiB 校验）
            if len(raw) > limit:
                self._commit_oversize(epoch_id, uid, len(raw))
                counters["oversize"] += 1
                return None

            # 4) 解析并提取全部 application/pdf（不按文件名/Message-ID 去重）
            try:
                parts = iter_pdf_parts(raw)
                headers = summarize_headers(raw)
            except Exception as exc:
                self._commit_failed(epoch_id, uid, len(raw), f"邮件解析失败: {exc!r}")
                counters["failed"] += 1
                return None

            # 5) 全部附件先落 staging（任一失败即整封放弃）
            for part in parts:
                storage.write_staged(staging, part)

            # 6) 整封原子发布到正式目录
            try:
                published = storage.publish_all(staging, parts, uidvalidity, uid)
            except OSError as exc:
                return f"UID {uid} 附件发布失败（磁盘错误）: {exc}"

            # 7) 索引 + 进度同一事务提交——提交后才可查询下载
            db.begin()
            try:
                message_pk = db.insert_message(
                    db._conn, epoch_id, uid, "done", None, len(raw),
                    headers.get("subject"), headers.get("from_header"),
                    headers.get("message_id_hdr"),
                )
                for part, (rel_path, size_bytes) in zip(parts, published):
                    db.insert_attachment(
                        db._conn, message_pk, part.part_number,
                        stored_filename(part), part.content_type,
                        size_bytes, rel_path,
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise

            counters["processed_ok"] += 1
            counters["attachments_saved"] += len(published)
            return None

        except ImapError as exc:
            # 断流、连接重置等：不推进该封、中止本轮（临时件由 finally 清理）
            return f"UID {uid} 取信中止: {exc}"
        except OSError as exc:
            return f"UID {uid} 落盘失败: {exc}"
        except Exception as exc:
            # 未预料错误：保守中止，避免反复产生脏状态
            return f"UID {uid} 未预期错误: {exc!r}"
        finally:
            # 无论成功、失败、断流还是进程异常路径，该封 staging 必须清空，
            # 绝不留下任何半封附件痕迹
            storage.discard(staging)

    # ------------------------------------------------------------------
    def _commit_oversize(self, epoch_id: int, uid: int, size: int) -> None:
        db = self._db
        db.begin()
        try:
            db.insert_message(
                db._conn, epoch_id, uid, "oversize",
                f"邮件 {size} 字节，超过上限 {self._settings.max_message_bytes}",
                size, None, None, None,
            )
            db.commit()
        except Exception:
            db.rollback()
            raise

    def _commit_failed(self, epoch_id: int, uid: int, size: int,
                       reason: str) -> None:
        db = self._db
        db.begin()
        try:
            db.insert_message(
                db._conn, epoch_id, uid, "failed", reason,
                size, None, None, None,
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
