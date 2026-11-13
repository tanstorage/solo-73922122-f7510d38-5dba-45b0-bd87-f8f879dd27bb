"""IMAP 演示服务启动入口：python -m imapdemo"""
from __future__ import annotations

import asyncio
import logging
import os

from .server import start_server
from .state import MailState


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    data_dir = os.environ.get("MAIL_DATA_DIR", "/maildata")
    host = os.environ.get("IMAP_BIND_HOST", "0.0.0.0")
    port = int(os.environ.get("IMAP_BIND_PORT", "143"))
    user = os.environ.get("IMAP_USER", "invoicing@example.com")
    password = os.environ.get("IMAP_PASSWORD", "invoice-pass")

    state = MailState(data_dir)

    async def run() -> None:
        server = await start_server(host, port, state, user, password)
        async with server:
            await server.serve_forever()

    asyncio.run(run())


if __name__ == "__main__":
    main()
