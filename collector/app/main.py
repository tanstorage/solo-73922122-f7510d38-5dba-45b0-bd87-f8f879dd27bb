"""FastAPI 入口：采集触发、进度查询、按来源（UIDVALIDITY + UID）下载。

启动后访问 /docs 可看到全部入口并在线试用。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .collector import Collector
from .config import load_settings
from .database import Database
from .process_lock import CollectionBusyError
from .storage import Storage


# ---- 响应模型（/docs 中展示结构）----
class AttachmentOut(BaseModel):
    id: int
    part_number: str
    filename: str
    content_type: str
    size_bytes: int
    download_url: str


class MessageOut(BaseModel):
    id: int
    uidvalidity: int
    uid: int
    status: str
    failure_reason: Optional[str]
    message_size: Optional[int]
    subject: Optional[str]
    from_header: Optional[str]
    message_id_hdr: Optional[str]
    collected_at: Optional[str]
    attachment_count: int
    attachments: list[AttachmentOut] = Field(default_factory=list)


class EpochOut(BaseModel):
    id: int
    uidvalidity: int
    mailbox: str
    observed_at: str
    total: int
    done: int
    failed: int
    oversize: int
    attachments: int


class RunOut(BaseModel):
    id: int
    uidvalidity: Optional[int]
    started_at: str
    finished_at: Optional[str]
    status: str
    total_seen: int
    processed_ok: int
    failed: int
    oversize: int
    skipped_terminal: int
    attachments_saved: int
    note: Optional[str]


class CollectResponse(BaseModel):
    run_id: int
    status: str
    abort_reason: Optional[str]
    counters: dict


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    db = Database(settings.db_path)
    storage = Storage(settings.blob_root, settings.staging_dir)
    # 清理上次进程崩溃遗留的 staging 临时件
    removed = storage.cleanup_stale_staging()
    state["settings"] = settings
    state["db"] = db
    state["storage"] = storage
    state["collector"] = Collector(settings, db, storage)
    state["stale_staging_cleaned"] = removed
    yield
    db.close()


app = FastAPI(
    title="共享收票邮箱 PDF 采集 API",
    description=(
        "从固定 IMAP 邮箱以 UID 采集发票 PDF 原件。\n\n"
        "- 只读 EXAMINE + BODY.PEEK，不改变邮件已读状态；\n"
        "- 每封 ≤ 8MiB，提取全部 application/pdf，按 MIME 部分序号区分同名附件；\n"
        "- 附件全部落盘后，索引与进度同事务提交，绝不暴露半封附件；\n"
        "- 邮箱重建（UIDVALIDITY 变化）后在新 UID 空间重新采集，旧原件仍可查询。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _db() -> Database:
    return state["db"]


def _storage() -> Storage:
    return state["storage"]


def _attachment_out(row) -> AttachmentOut:
    return AttachmentOut(
        id=row["id"], part_number=row["part_number"], filename=row["filename"],
        content_type=row["content_type"], size_bytes=row["size_bytes"],
        download_url=f"/api/messages/attachments/{row['id']}/download",
    )


def _message_out(row, attachments: Optional[list] = None) -> MessageOut:
    if attachments is None:
        attachments = _db().list_attachments(row["id"])
    return MessageOut(
        id=row["id"], uidvalidity=row["uidvalidity"], uid=row["uid"],
        status=row["status"], failure_reason=row["failure_reason"],
        message_size=row["message_size"], subject=row["subject"],
        from_header=row["from_header"], message_id_hdr=row["message_id_hdr"],
        collected_at=row["collected_at"], attachment_count=len(attachments),
        attachments=[_attachment_out(a) for a in attachments],
    )


# ---------------------------------------------------------------- 健康检查
@app.get("/health", tags=["系统"], summary="健康检查")
def health() -> dict:
    return {
        "status": "ok",
        "stale_staging_cleaned_on_boot": state.get("stale_staging_cleaned", 0),
    }


# ---------------------------------------------------------------- 采集触发
@app.post("/api/collect", response_model=CollectResponse, tags=["采集"],
          summary="触发一轮采集（全进程唯一，并发触发返回 409）")
def trigger_collect() -> CollectResponse:
    collector: Collector = state["collector"]
    try:
        result = collector.run_collection()
    except CollectionBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return CollectResponse(**result)


@app.get("/api/runs", response_model=list[RunOut], tags=["采集"],
         summary="历史采集运行记录")
def list_runs(limit: int = 50) -> list[RunOut]:
    return [RunOut(**dict(r)) for r in _db().list_runs(limit)]


# ---------------------------------------------------------------- 进度 / 代际
@app.get("/api/progress", response_model=list[EpochOut], tags=["进度"],
         summary="各 UIDVALIDITY 代际的采集进度（含邮箱重建后的新代际）")
def progress() -> list[EpochOut]:
    result = []
    for epoch in _db().list_epochs():
        p = _db().progress_for_epoch(epoch["id"])
        result.append(EpochOut(
            id=epoch["id"], uidvalidity=epoch["uidvalidity"],
            mailbox=epoch["mailbox"], observed_at=epoch["observed_at"],
            total=p["total"] or 0, done=p["done"] or 0,
            failed=p["failed"] or 0, oversize=p["oversize"] or 0,
            attachments=p["attachments"] or 0,
        ))
    return result


@app.get("/api/epochs", response_model=list[EpochOut], tags=["进度"],
         summary="邮箱代际列表（每次 UIDVALIDITY 变化一条）")
def list_epochs() -> list[EpochOut]:
    return progress()


# ---------------------------------------------------------------- 邮件与来源
@app.get("/api/messages", response_model=list[MessageOut], tags=["来源查询"],
         summary="已采集邮件列表，可按 UIDVALIDITY 代际/状态过滤")
def list_messages(uidvalidity: Optional[int] = None,
                  status: Optional[str] = None,
                  limit: int = 200) -> list[MessageOut]:
    epoch_id = None
    if uidvalidity is not None:
        epoch = _db().query_one(
            "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
        )
        if epoch is None:
            return []
        epoch_id = epoch["id"]
    rows = _db().list_messages(epoch_id=epoch_id, status=status, limit=limit)
    return [_message_out(r) for r in rows]


@app.get("/api/messages/{message_pk}", response_model=MessageOut,
         tags=["来源查询"], summary="按内部 ID 查询单封邮件及其附件来源")
def get_message(message_pk: int) -> MessageOut:
    row = _db().get_message_with_epoch(message_pk)
    if row is None:
        raise HTTPException(404, "邮件记录不存在")
    return _message_out(row)


@app.get("/api/sources/{uidvalidity}/{uid}", response_model=MessageOut,
         tags=["来源查询"], summary="按来源（UIDVALIDITY + UID）定位邮件")
def get_source(uidvalidity: int, uid: int) -> MessageOut:
    epoch = _db().query_one(
        "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
    )
    if epoch is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity} 代际不存在")
    row = _db().get_message(epoch["id"], uid)
    if row is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity}, UID={uid} 无记录")
    full = _db().get_message_with_epoch(row["id"])
    return _message_out(full)


# ---------------------------------------------------------------- 下载
@app.get("/api/messages/attachments/{attachment_pk}/download",
         tags=["下载"], summary="按附件 ID 下载 PDF 原件")
def download_attachment(attachment_pk: int):
    row = _db().get_attachment(attachment_pk)
    if row is None:
        raise HTTPException(404, "附件不存在")
    return _serve_blob(row["storage_path"], row["filename"])


@app.get("/api/sources/{uidvalidity}/{uid}/download",
         tags=["下载"],
         summary="按来源（UIDVALIDITY + UID）下载该封全部 PDF（ZIP 打包）")
def download_source_bundle(uidvalidity: int, uid: int):
    import io
    import zipfile

    epoch = _db().query_one(
        "SELECT id FROM mailbox_epochs WHERE uidvalidity=?", (uidvalidity,)
    )
    if epoch is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity} 代际不存在")
    msg = _db().get_message(epoch["id"], uid)
    if msg is None:
        raise HTTPException(404, f"UIDVALIDITY={uidvalidity}, UID={uid} 无记录")
    attachments = _db().list_attachments(msg["id"])
    if not attachments:
        raise HTTPException(404, "该邮件没有已落盘的 PDF 附件")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for att in attachments:
            abs_path = _storage().absolute_path(att["storage_path"])
            # 防目录穿越：正式文件必须位于 blob_root 之内
            if not os.path.isfile(abs_path):
                raise HTTPException(410, f"原件缺失: {att['storage_path']}")
            zf.write(abs_path, arcname=att["filename"])
    buffer.seek(0)
    from fastapi.responses import Response
    return Response(
        content=buffer.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition":
                f'attachment; filename="uv-{uidvalidity}-uid-{uid}.zip"'
        },
    )


def _serve_blob(storage_path: str, download_name: str):
    storage = _storage()
    abs_path = storage.absolute_path(storage_path)
    # 规范化后必须仍在 blob_root 内，防止任何路径穿越
    root = os.path.realpath(storage.blob_root)
    target = os.path.realpath(abs_path)
    if os.path.commonpath([root, target]) != root or not os.path.isfile(target):
        raise HTTPException(404, "原件不可用")
    return FileResponse(
        target, media_type="application/pdf",
        filename=download_name,
    )
