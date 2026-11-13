"""从 RFC822 邮件原文中提取全部 application/pdf 附件。

MIME 部分序号遵循 IMAP/RFC 3501 约定：
  根消息为 1；multipart 的第 n 个子部分为 "<父序号>.n"；
  message/rfc822 内嵌消息整体占一个子部分，其内部再向下编号。
同名附件因此可通过部分序号（如 1.2.1 与 1.2.2）稳定区分。
不按文件名或 Message-ID 做任何去重。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from email.header import decode_header, make_header
from email.message import Message
from email.policy import default as default_policy
from typing import Iterator, Optional


@dataclass(frozen=True)
class PdfPart:
    part_number: str          # 例如 "1.2.1"
    filename: str             # 归一化后的文件名（不含部分序号）
    content_type: str         # 规范化后的 MIME 类型，小写
    payload: bytes


def load_message(raw: bytes) -> Message:
    from email import message_from_bytes
    return message_from_bytes(raw, policy=default_policy)


def _is_pdf(part: Message) -> bool:
    # content_type 已由 policy 规范化为小写（如 application/pdf）
    if part.get_content_type().lower() == "application/pdf":
        return True
    # 对声明不规范的邮件，再用文件名兜底判断一次
    filename = part.get_filename()
    return bool(filename) and filename.lower().endswith(".pdf")


def _walk_parts(part: Message, number: str) -> Iterator[tuple[str, Message]]:
    """深度优先遍历，产出 (IMAP 部分序号, 叶子/附件部分)。"""
    if part.is_multipart():
        for index, child in enumerate(part.get_payload(), start=1):
            child_no = f"{number}.{index}"
            yield from _walk_parts(child, child_no)
        return

    content_type = part.get_content_type().lower()
    if content_type == "message/rfc822":
        # 内嵌消息整体是一个子部分；其内部头部/正文从该序号继续向下编号
        payload = part.get_payload()
        inner = payload[0] if isinstance(payload, list) else payload
        if isinstance(inner, Message):
            yield from _walk_parts(inner, number)
            return
    yield number, part


def iter_pdf_parts(raw: bytes) -> list[PdfPart]:
    root = load_message(raw)
    results: list[PdfPart] = []
    for number, part in _walk_parts(root, "1"):
        if not _is_pdf(part):
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            # Content-Transfer-Encoding 无法解码时按原始字节处理
            payload = part.get_payload()
            if isinstance(payload, str):
                payload = payload.encode("latin-1", errors="replace")
        results.append(
            PdfPart(
                part_number=number,
                filename=_safe_filename(part.get_filename()),
                content_type=part.get_content_type().lower(),
                payload=payload if isinstance(payload, bytes) else b"",
            )
        )
    return results


def _decode_header_value(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename(raw_name: Optional[str]) -> str:
    name = _decode_header_value(raw_name).strip()
    # 去掉任何路径成分，防止目录穿越
    name = name.replace("\\", "/").split("/")[-1]
    name = _SAFE_RE.sub("_", name)
    name = name.strip("._") or ""
    if not name:
        name = "unnamed.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


def stored_filename(part: PdfPart) -> str:
    """落盘文件名：部分序号前缀确保同名附件互不覆盖。"""
    return f"{part.part_number}__{part.filename}"


def summarize_headers(raw: bytes) -> dict[str, Optional[str]]:
    msg = load_message(raw)
    return {
        "subject": _decode_header_value(msg.get("Subject")) or None,
        "from_header": _decode_header_value(msg.get("From")) or None,
        "message_id_hdr": (msg.get("Message-ID") or "").strip() or None,
    }
