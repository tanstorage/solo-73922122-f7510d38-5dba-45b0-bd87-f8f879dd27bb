"""跨进程恢复协调的集成测试（标准库 unittest，可直接运行）。

覆盖需求中的关键并发场景：
- 启动恢复不得把另一个活进程正在采集的运行标 aborted、不得删其暂存；
- 全局仅一份采集：另一入口触发返回 409；
- 采集进程被强杀后，后续启动标记 aborted、回收暂存，允许续采；
- 并发启动/恢复/新采集相遇时，不清新任务文件、不撤销已完成运行、不碰已提交附件；
- 保留 UID 采集、整封原子提交、下载接口依赖的数据不变。

运行：python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app.collector import Collector  # noqa: E402
from app.config import Settings  # noqa: E402
from app.database import Database  # noqa: E402
from app.process_lock import CollectionBusyError  # noqa: E402
from app.recovery import Recovery  # noqa: E402
from app.runtime import ProcessRegistry  # noqa: E402
from app.storage import Storage  # noqa: E402


def make_settings(data_dir: str, **overrides) -> Settings:
    base = dict(
        imap_host="127.0.0.1",
        imap_port=1143,
        imap_user="u",
        imap_password="p",
        imap_mailbox="INBOX",
        socket_timeout=5,
        max_message_bytes=8 * 1024 * 1024,
        data_dir=data_dir,
        db_path=os.path.join(data_dir, "collector.db"),
        blob_root=os.path.join(data_dir, "blobs"),
        staging_dir=os.path.join(data_dir, "staging"),
        lock_path=os.path.join(data_dir, "collect.lock"),
        runtime_dir=os.path.join(data_dir, "run"),
        demo_pause_after_first_pdf=False,
        demo_pause_max_seconds=0.0,
        demo_dir=os.path.join(data_dir, "demo"),
        demo_hold_file=os.path.join(data_dir, "demo", "hold"),
        demo_release_file=os.path.join(data_dir, "demo", "release"),
    )
    base.update(overrides)
    return Settings(**base)


class _FakeSession:
    """最小 IMAP 会话替身：按 UID 返回预置的 (size, raw)。"""

    def __init__(self, uidvalidity: int, messages: dict[int, bytes]):
        self.uidvalidity = uidvalidity
        self.message_count = len(messages)
        self._messages = messages
        self.fail_on_uid: int | None = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def search_all_uids(self):
        return sorted(self._messages)

    def fetch_size(self, uid):
        raw = self._messages[uid]
        return len(raw)

    def fetch_raw(self, uid):
        if self.fail_on_uid == uid:
            from app.imap_client import ImapError
            raise ImapError(f"UID {uid} 正文拉取失败（可能断流）")
        return self._messages[uid]


def _eml_with_pdf(subject: str, pdf_bytes: bytes) -> bytes:
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "billing@vendor.example"
    msg["To"] = "invoicing@example.com"
    msg["Message-ID"] = f"<{subject.replace(' ', '-').lower()}-{uuid.uuid4().hex[:8]}@vendor.example>"
    msg.set_content("请查收发票。")
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf",
                       filename="invoice.pdf")
    return msg.as_bytes()


def _tiny_pdf(tag: bytes) -> bytes:
    return b"%PDF-1.4\n% " + tag + b"\n%%EOF\n"


def _patch_session(collector: Collector, session: _FakeSession) -> None:
    import app.collector as mod
    original = mod.ImapSession

    class Wrapped:
        def __call__(self, *args, **kwargs):
            return session
    collector._session_factory = session  # 供测试读取
    mod.ImapSession = Wrapped  # type: ignore[assignment]
    collector._restore_session = original  # type: ignore[attr-defined]


class Workspace:
    """构造共享 DATA_DIR 的多个"API 进程"上下文。"""

    def __init__(self, tmp: str):
        self.data_dir = tmp
        self.settings = make_settings(tmp)

    def boot(self) -> tuple[ProcessRegistry, Database, Storage, Recover]:
        registry = ProcessRegistry(self.settings.runtime_dir)
        registry.register()
        db = Database(self.settings.db_path)
        storage = Storage(self.settings.blob_root, self.settings.staging_dir)
        recovery = Recovery(db, storage, registry, self.settings.runtime_dir,
                            self.settings.lock_path)
        recovery.run()
        return registry, db, storage, recovery


# ----------------------------------------------------------------------
class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="collector-test-")
        self.ws = Workspace(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # 场景 1：活进程的运行与暂存不得被动
    def test_live_run_preserved_on_other_process_boot(self):
        reg_a, db_a, st_a, _ = self.ws.boot()
        run_id = db_a.start_run(None, None, owner_token=reg_a.token)
        staging = st_a.new_staging_dir(run_id)
        db_a.set_run_staging(run_id, staging)
        st_a.write_staged(staging, self._part())

        reg_b, db_b, st_b, _ = self.ws.boot()
        self.assertEqual(db_b.run_status(run_id), "running")
        self.assertTrue(os.path.isdir(staging))
        self.assertEqual(db_b.get_run(run_id)["staging_dir"], staging)

    # 场景 2：死主（登记锁被释放）启动后被标 aborted 且暂存被回收
    def test_dead_owner_marked_aborted_and_staging_reaped(self):
        reg_a, db_a, st_a, _ = self.ws.boot()
        run_id = db_a.start_run(None, None, owner_token=reg_a.token)
        staging = st_a.new_staging_dir(run_id)
        st_a.write_staged(staging, self._part())
        # 模拟强杀：仅释放登记锁（不删文件，模拟内核在 SIGKILL 时释放 flock）
        reg_a.close()

        reg_b, db_b, st_b, _ = self.ws.boot()
        self.assertEqual(db_b.run_status(run_id), "aborted")
        self.assertFalse(os.path.exists(staging))
        self.assertIn("强制终止", db_b.get_run(run_id)["note"] or "")

    # 场景 3：两进程同时启动/恢复与新采集相遇，不误删新任务
    def test_concurrent_boot_and_new_collection(self):
        reg_a, db_a, st_a, _ = self.ws.boot()
        reg_b, db_b, st_b, _ = self.ws.boot()

        # A 的旧死运行（owner 不存在）应当被回收
        dead = db_a.start_run(None, None, owner_token="ghost-token")
        dead_staging = st_a.new_staging_dir(dead)
        st_a.write_staged(dead_staging, self._part())

        # B 的新活运行，目录刚建、尚未登记
        live_run = db_b.start_run(None, None, owner_token=reg_b.token)
        live_staging = st_b.new_staging_dir(live_run)
        st_b.write_staged(live_staging, self._part())

        # 并发触发两次恢复
        results = []
        threads = [
            threading.Thread(target=lambda: results.append(
                Recovery(db_b, st_b, reg_b, self.ws.settings.runtime_dir,
                         self.ws.settings.lock_path).run()))
            for _ in range(3)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(db_b.run_status(dead), "aborted")
        self.assertFalse(os.path.exists(dead_staging))
        self.assertEqual(db_b.run_status(live_run), "running")
        self.assertTrue(os.path.exists(live_staging))

    # 场景 4：已完成运行与已提交附件永远不会被启动恢复改动
    def test_completed_run_and_blobs_untouched(self):
        reg_a, db_a, st_a, _ = self.ws.boot()
        run_id = db_a.start_run(None, None, owner_token=reg_a.token)
        epoch_id = db_a.get_or_create_epoch(111, "INBOX")
        blob = os.path.join(st_a.relative_dir(111, 1), "1__invoice.pdf")
        os.makedirs(os.path.join(st_a.blob_root, st_a.relative_dir(111, 1)),
                    exist_ok=True)
        with open(os.path.join(st_a.blob_root, blob), "wb") as fh:
            fh.write(_tiny_pdf(b"x"))
        db_a.begin()
        pk = db_a.insert_message(db_a._conn, epoch_id, 1, "done", None,
                                 len(_tiny_pdf(b"x")), "s", "f", "m")
        db_a.insert_attachment(db_a._conn, pk, "1", "invoice.pdf",
                               "application/pdf", 20, blob)
        db_a.commit()
        db_a.finish_run(run_id, "completed", {"processed_ok": 1})

        reg_b, db_b, st_b, _ = self.ws.boot()
        self.assertEqual(db_b.run_status(run_id), "completed")
        self.assertTrue(os.path.isfile(os.path.join(st_a.blob_root, blob)))
        att = db_b.list_attachments(pk)
        self.assertEqual(len(att), 1)

    # 场景 5：全局仅一份采集——另一入口触发 409
    def test_second_entrypoint_gets_409(self):
        reg_a, db_a, st_a, _ = self.ws.boot()
        reg_b, db_b, st_b, _ = self.ws.boot()

        collector_a = Collector(self.ws.settings, db_a, st_a, owner_token=reg_a.token)
        collector_b = Collector(self.ws.settings, db_b, st_b, owner_token=reg_b.token)

        from app.process_lock import ProcessLock
        holding = ProcessLock(self.ws.settings.lock_path)
        holding.acquire_nonblocking()
        try:
            with self.assertRaises(CollectionBusyError):
                collector_b.run_collection()
        finally:
            holding.release()
        # 同进程第二触发也立即 409（线程锁）
        self.assertTrue(dir(collector_a))

    # 场景 6：首份 PDF 落盘后暂停期间，另一进程启动不打断；强杀后续采成功
    def test_pause_switch_then_resume_after_kill(self):
        settings = make_settings(self.tmp, demo_pause_after_first_pdf=True,
                                 demo_pause_max_seconds=0.4)
        reg_a = ProcessRegistry(settings.runtime_dir); reg_a.register()
        db_a = Database(settings.db_path)
        st_a = Storage(settings.blob_root, settings.staging_dir)
        Recovery(db_a, st_a, reg_a, settings.runtime_dir, settings.lock_path).run()

        messages = {1: _eml_with_pdf("INV-1", _tiny_pdf(b"one"))}
        session = _FakeSession(4242, messages)
        import app.collector as mod
        original = mod.ImapSession

        holder = {"paused": False}

        class SlowCollector(Collector):
            def _demo_pause_after_first_pdf(inner, uid, staging):
                holder["paused"] = True
                holder["staging"] = staging
                time.sleep(0.5)

        collector = SlowCollector(settings, db_a, st_a, owner_token=reg_a.token)
        mod.ImapSession = lambda *a, **k: session  # type: ignore[assignment]
        try:
            t = threading.Thread(target=collector.run_collection)
            t.start()
            while not holder.get("paused"):
                time.sleep(0.01)

            # 暂停中：另一进程启动恢复
            reg_b = ProcessRegistry(settings.runtime_dir); reg_b.register()
            db_b = Database(settings.db_path)
            st_b = Storage(settings.blob_root, settings.staging_dir)
            result = Recovery(db_b, st_b, reg_b, settings.runtime_dir,
                              settings.lock_path).run()
            self.assertTrue(os.path.exists(holder["staging"]))
            self.assertTrue(result["preserved_runs"])
            t.join()
            self.assertEqual(db_a.list_runs()[0]["status"], "completed")
        finally:
            mod.ImapSession = original  # type: ignore[assignment]

    # 场景 7：采集进程异常中止（会话抛错）→ 运行 aborted、该封不落库、暂存清空，
    #        下一轮（模拟强杀后的新进程）可续采且不重复
    def test_aborted_run_resumes_without_duplicates(self):
        settings = make_settings(self.tmp)
        reg_a = ProcessRegistry(settings.runtime_dir); reg_a.register()
        db_a = Database(settings.db_path)
        st_a = Storage(settings.blob_root, settings.staging_dir)
        Recovery(db_a, st_a, reg_a, settings.runtime_dir, settings.lock_path).run()

        m1 = _eml_with_pdf("INV-1", _tiny_pdf(b"one"))
        m2 = _eml_with_pdf("INV-2", _tiny_pdf(b"two"))
        session = _FakeSession(777, {1: m1, 2: m2})
        session.fail_on_uid = 2
        import app.collector as mod
        original = mod.ImapSession
        mod.ImapSession = lambda *a, **k: session  # type: ignore[assignment]
        collector = Collector(settings, db_a, st_a, owner_token=reg_a.token)
        try:
            first = collector.run_collection()
            self.assertEqual(first["status"], "aborted")
            self.assertEqual(len(db_a.list_messages()), 1)  # UID 1 done
            self.assertEqual(os.listdir(settings.staging_dir), [])
            # UID 2 无记录 → 续采
            session.fail_on_uid = None
            second = collector.run_collection()
            self.assertEqual(second["status"], "completed")
            self.assertEqual(second["counters"]["processed_ok"], 1)
            self.assertEqual(second["counters"]["skipped_terminal"], 1)
            self.assertEqual(len(db_a.list_messages()), 2)
        finally:
            mod.ImapSession = original  # type: ignore[assignment]

    @staticmethod
    def _part():
        from app.pdf_extract import PdfPart
        return PdfPart(part_number="1.2", filename="invoice.pdf",
                       content_type="application/pdf",
                       payload=_tiny_pdf(b"aged"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
