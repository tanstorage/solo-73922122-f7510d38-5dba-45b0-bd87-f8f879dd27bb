"""IMAP 客户端封装。

约束：
- 只用 EXAMINE 以只读方式打开邮箱，绝不修改邮件的 \\Seen 已读标志；
- 全程使用 UID 协议（UID SEARCH / UID FETCH），邮箱删信导致的序号变化不影响采集；
- 不做任何 STORE / EXPUNGE / APPEND 写操作。
"""
from __future__ import annotations

import imaplib
import re
import socket
from typing import Optional

UID_RE = re.compile(rb"UID\s+(\d+)")
SIZE_RE = re.compile(rb"RFC822\.SIZE\s+(\d+)")


class ImapError(RuntimeError):
    """IMAP 通信层面的错误（连接、登录、取信失败等），本轮采集应中止。"""


class ImapSession:
    def __init__(self, host: str, port: int, user: str, password: str,
                 mailbox: str, timeout: int) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._mailbox = mailbox
        self._timeout = timeout
        self._client: Optional[imaplib.IMAP4] = None
        self.uidvalidity: Optional[int] = None
        self.message_count: int = 0

    # ---- 连接生命周期 ----
    def open(self) -> None:
        try:
            socket.setdefaulttimeout(self._timeout)
            client = imaplib.IMAP4(self._host, self._port, timeout=self._timeout)
            client.login(self._user, self._password)
            # select(readonly=True) 实际发送 EXAMINE：只读打开，不会改动 \\Seen
            typ, data = client.select(self._mailbox, readonly=True)
            if typ != "OK":
                raise ImapError(f"EXAMINE {self._mailbox} 失败: {data}")
            self._client = client
            # imaplib 把响应码解析到 untagged_responses：UIDVALIDITY / EXISTS
            uv = client.untagged_responses.get("UIDVALIDITY")
            if not uv:
                raise ImapError("服务端 SELECT/EXAMINE 响应缺少 UIDVALIDITY")
            self.uidvalidity = int(uv[0])
            exists = client.untagged_responses.get("EXISTS")
            self.message_count = int(exists[0]) if exists else 0
        except (imaplib.IMAP4.error, OSError) as exc:
            self.close()
            raise ImapError(f"IMAP 连接/登录失败: {exc}") from exc

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.logout()
            except Exception:
                pass
            self._client = None

    def __enter__(self) -> "ImapSession":
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- 协议操作 ----
    def search_all_uids(self) -> list[int]:
        """UID SEARCH ALL —— 返回当前 UID 空间内全部邮件 UID。"""
        assert self._client is not None
        try:
            typ, data = self._client.uid("SEARCH", "ALL")
        except (imaplib.IMAP4.error, OSError) as exc:
            raise ImapError(f"UID SEARCH 失败: {exc}") from exc
        if typ != "OK":
            raise ImapError(f"UID SEARCH 返回非 OK: {data}")
        payload = data[0] if data and data[0] else b""
        return [int(x) for x in payload.split()]

    def fetch_size(self, uid: int) -> Optional[int]:
        """先取 RFC822.SIZE，超限邮件不拉正文，节省带宽且双重保险。"""
        assert self._client is not None
        try:
            typ, data = self._client.uid(
                "FETCH", str(uid), "(UID RFC822.SIZE)"
            )
        except (imaplib.IMAP4.error, OSError) as exc:
            raise ImapError(f"UID {uid} SIZE 拉取失败: {exc}") from exc
        if typ != "OK" or not data:
            raise ImapError(f"UID {uid} SIZE 响应异常: {data}")
        joined = b" ".join(p for p in data if isinstance(p, bytes))
        match = SIZE_RE.search(joined)
        return int(match.group(1)) if match else None

    def fetch_raw(self, uid: int) -> bytes:
        """BODY.PEEK[].  PEEK 保证不设置 \\Seen。返回完整 RFC822 原文。"""
        assert self._client is not None
        try:
            typ, data = self._client.uid(
                "FETCH", str(uid), "(UID BODY.PEEK[])"
            )
        except (imaplib.IMAP4.error, OSError) as exc:
            # 断流（对端中途关闭）会在这里表现为 socket 错误 / 未完整响应
            raise ImapError(f"UID {uid} 正文拉取失败（可能断流）: {exc}") from exc
        if typ != "OK":
            raise ImapError(f"UID {uid} 正文响应非 OK: {data}")
        for item in data:
            # 文字段与字面量以 (b'... (UID n BODY[] {len}', <bytes>) 元组返回
            if isinstance(item, tuple) and len(item) == 2 and isinstance(item[1], bytes):
                header = item[0] or b""
                uid_match = UID_RE.search(header)
                if uid_match and int(uid_match.group(1)) == uid:
                    return item[1]
        raise ImapError(f"UID {uid} 响应中未找到邮件正文字面量")
