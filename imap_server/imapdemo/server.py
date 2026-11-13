"""最小可用的本地 IMAP4rev1 演示服务（asyncio，零第三方依赖）。

仅实现采集方需要的只读子集：
  CAPABILITY / LOGIN / EXAMINE(SELECT 同义，只读) /
  UID SEARCH ALL / UID FETCH (UID, RFC822.SIZE, FLAGS, INTERNALDATE, BODY.PEEK[]) /
  NOOP / LOGOUT

关键演示能力：
- EXAMINE 响应携带 UIDVALIDITY，且绝不写入 \\Seen；
- 对 flaky_uid 执行 BODY.PEEK[] 时，发送部分字面量后直接断开，模拟多附件中途断流；
- 状态每条命令前从磁盘重新加载，CLI 的删信/重建/断流设置即时生效。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .state import MailState

LOG = logging.getLogger("imap-demo")

CAPABILITIES = ("IMAP4rev1", "UIDPLUS", "AUTH=PLAIN", "AUTH=LOGIN")


class ImapClientHandler:
    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter, state: MailState,
                 user: str, password: str) -> None:
        self.reader = reader
        self.writer = writer
        self.state = state
        self.user = user
        self.password = password
        self.selected = False
        self.authenticated = False
        self.peer = writer.get_extra_info("peername")

    # ---- 基础收发 ----
    async def send(self, text: str) -> None:
        LOG.debug("-> %s", text.strip())
        self.writer.write(text.encode("utf-8"))
        await self.writer.drain()

    async def send_raw(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    async def read_command(self) -> Optional[tuple[str, list[str]]]:
        line = await self.reader.readline()
        if not line:
            return None
        text = line.decode("utf-8", errors="replace").rstrip("\r\n")
        LOG.debug("<- %s", text)
        tokens = tokenize(text)
        if not tokens:
            return None
        tag = tokens[0]
        return tag, _normalize_tokens(tokens[1:])

    # ---- 主循环 ----
    async def serve(self) -> None:
        host, port = self.peer[:2] if self.peer else ("?", "?")
        LOG.info("连接来自 %s:%s", host, port)
        await self.send("* OK IMAP4rev1 demo server ready\r\n")
        try:
            while True:
                parsed = await self.read_command()
                if parsed is None:
                    break
                tag, tokens = parsed
                if not tokens:
                    await self.send(f"{tag} BAD empty command\r\n")
                    continue
                command = tokens[0].upper()
                args = tokens[1:]
                done = await self.dispatch(tag, command, args)
                if done:
                    break
        except (ConnectionError, asyncio.IncompleteReadError, BrokenPipeError):
            pass
        except Exception:
            LOG.exception("连接处理异常")
        finally:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            LOG.info("连接关闭 %s:%s", host, port)

    async def dispatch(self, tag: str, command: str, args: list[str]) -> bool:
        if command == "CAPABILITY":
            caps = " ".join(c for c in CAPABILITIES if c)
            await self.send(f"* CAPABILITY {caps}\r\n{tag} OK CAPABILITY completed\r\n")
        elif command == "LOGOUT":
            await self.send(f"* BYE server logging out\r\n{tag} OK LOGOUT completed\r\n")
            return True
        elif command == "NOOP":
            await self.send(f"{tag} OK NOOP completed\r\n")
        elif command == "LOGIN":
            if len(args) < 2:
                await self.send(f"{tag} BAD LOGIN requires user and password\r\n")
            elif self.authenticated:
                await self.send(f"{tag} BAD already authenticated\r\n")
            elif args[0] == self.user and args[1] == self.password:
                self.authenticated = True
                await self.send(f"{tag} OK LOGIN completed\r\n")
            else:
                await self.send(f"{tag} NO login failed\r\n")
        elif command in ("EXAMINE", "SELECT"):
            # 演示服务中 EXAMINE/SELECT 都按只读处理，永不改标志
            snap = self.state.snapshot()
            exists = len(snap["messages"])
            recent = sum(1 for m in snap["messages"] if "\\Recent" in m.get("flags", []))
            uidnext = snap["next_uid"]
            uidvalidity = snap["uidvalidity"]
            await self.send(f"* {exists} EXISTS\r\n")
            await self.send(f"* {recent} RECENT\r\n")
            await self.send("* FLAGS (\\Seen \\Answered \\Flagged \\Deleted \\Draft)\r\n")
            await self.send(
                f"* OK [UIDVALIDITY {uidvalidity}] UIDVALIDITY of mailbox\r\n"
            )
            await self.send(f"* OK [UIDNEXT {uidnext}] next uid\r\n")
            await self.send(
                f"* OK [PERMANENTFLAGS ()] read-only mailbox\r\n"
            )
            self.selected = True
            await self.send(f"{tag} OK [READ-ONLY] {command} completed\r\n")
        elif command == "UID":
            await self.handle_uid(tag, args)
        else:
            await self.send(f"{tag} BAD unsupported command: {command}\r\n")
        return False

    # ---- UID 子命令 ----
    async def handle_uid(self, tag: str, args: list[str]) -> None:
        if not self.selected:
            await self.send(f"{tag} BAD no mailbox selected\r\n")
            return
        if not args:
            await self.send(f"{tag} BAD UID requires subcommand\r\n")
            return
        sub = args[0].upper()
        if sub == "SEARCH":
            # 采集方只用 UID SEARCH ALL；演示服务把其它准则也当作 ALL 处理
            snap = self.state.snapshot()
            uids = " ".join(str(m["uid"]) for m in snap["messages"])
            await self.send(f"* SEARCH {uids}\r\n{tag} OK UID SEARCH completed\r\n")
        elif sub == "FETCH":
            await self.handle_uid_fetch(tag, args[1:])
        else:
            await self.send(f"{tag} BAD unsupported UID subcommand: {sub}\r\n")

    async def handle_uid_fetch(self, tag: str, args: list[str]) -> None:
        if not args:
            await self.send(f"{tag} BAD UID FETCH requires uid set\r\n")
            return
        uid_set = args[0]
        snap = self.state.snapshot()
        by_uid = {int(m["uid"]): m for m in snap["messages"]}
        wanted = expand_uid_set(uid_set, by_uid.keys())
        wants_body = any("BODY" in a.upper() for a in args[1:])
        wants_meta = any(
            key in " ".join(args[1:]).upper()
            for key in ("RFC822.SIZE", "INTERNALDATE", "FLAGS", "UID")
        ) or not wants_body

        for uid in wanted:
            msg = by_uid.get(uid)
            if msg is None:
                continue
            seq = sorted(by_uid.keys()).index(uid) + 1
            flags = " ".join(msg.get("flags", []))
            if wants_meta:
                meta = (
                    f"* {seq} FETCH (UID {uid} RFC822.SIZE {msg['size']} "
                    f"FLAGS ({flags}) INTERNALDATE \"{msg['internaldate']}\")\r\n"
                )
                await self.send(meta)
            if wants_body:
                eml = self.state.read_eml(msg)
                flaky = snap.get("flaky_uid")
                if flaky is not None and int(flaky) == uid:
                    # 演示：多附件邮件中途断流——只发一半字节然后断开连接
                    header = (
                        f"* {seq} FETCH (UID {uid} BODY[] {{{len(eml)}}}\r\n"
                    ).encode("utf-8")
                    await self.send_raw(header)
                    half = eml[: len(eml) // 2]
                    await self.send_raw(half)
                    LOG.warning("断流演示：UID %s 发送 %d/%d 字节后主动断开",
                                uid, len(half), len(eml))
                    self.writer.close()
                    try:
                        await self.writer.wait_closed()
                    except Exception:
                        pass
                    return
                header = (
                    f"* {seq} FETCH (UID {uid} BODY[] {{{len(eml)}}}\r\n"
                ).encode("utf-8")
                # 字面量后直接接 ')'，不能再插 CRLF，否则客户端会把 ')' 当成独立响应
                await self.send_raw(header + eml + b")\r\n")
        await self.send(f"{tag} OK UID FETCH completed\r\n")


# ---------------------------------------------------------------- 工具函数
def tokenize(line: str) -> list[str]:
    """IMAP 词法切分：支持引号字符串与括号列表（本服务无需字面量入参）。"""
    tokens: list[str] = []
    buf = ""
    in_quotes = False
    for ch in line:
        if ch == '"':
            in_quotes = not in_quotes
            continue
        if ch in (" ", "\t") and not in_quotes:
            if buf:
                tokens.append(buf)
                buf = ""
            continue
        buf += ch
    if buf:
        tokens.append(buf)
    return tokens


def _normalize_tokens(tokens: list[str]) -> list[str]:
    # 命令名固定大写；括号内的 FETCH 项保持原样交由上层判断
    return [tokens[0].upper()] + tokens[1:]


def expand_uid_set(uid_set: str, available) -> list[int]:
    """支持 n、n,m 与 n:m（* 表示最大 UID）。"""
    available = sorted(int(u) for u in available)
    if not available:
        return []
    result: list[int] = []
    for piece in uid_set.split(","):
        piece = piece.strip()
        if piece == "*":
            result.append(available[-1])
        elif ":" in piece:
            lo_s, hi_s = piece.split(":", 1)
            lo = available[0] if lo_s == "*" else int(lo_s)
            hi = available[-1] if hi_s == "*" else int(hi_s)
            result.extend(u for u in available if min(lo, hi) <= u <= max(lo, hi))
        else:
            uid = int(piece)
            if uid in available:
                result.append(uid)
    return result


async def start_server(host: str, port: int, state: MailState,
                       user: str, password: str) -> asyncio.base_events.Server:
    async def factory(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        handler = ImapClientHandler(reader, writer, state, user, password)
        await handler.serve()

    server = await asyncio.start_server(factory, host, port)
    LOG.info("IMAP 演示服务监听 %s:%s", host, port)
    return server
