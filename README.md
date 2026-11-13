# 共享收票邮箱 PDF 采集服务

从固定配置的共享收票邮箱（IMAP）归集发票 PDF 原件，供后续发票识别入仓。
技术栈：**FastAPI + SQLite + 本地文件存储**，并附带一个零第三方依赖的**本地 IMAP 演示服务**。

## 能力与约束对照

| 需求 | 实现 |
| --- | --- |
| IMAP 地址/账号/INBOX 配置固定 | `collector/app/config.py`，全部来自环境变量，`docker-compose.yml` 固定注入 |
| 以 UID 取信 | 全程 `UID SEARCH` / `UID FETCH`，不依赖邮件序号 |
| 不改变已读状态 | `SELECT ... (readonly)` 实际发送 `EXAMINE`，正文用 `BODY.PEEK[]`，无任何 STORE/EXPUNGE |
| 每封最多 8 MiB | 先查 `RFC822.SIZE` 预校验，拉到原文再按实际字节二次校验；超限标记 `oversize`、不拉/不留正文 |
| 提取全部 `application/pdf` | 深度遍历 MIME 树，非 PDF 附件忽略，不按文件名或 Message-ID 去重 |
| 同名附件区分 | 按 IMAP MIME 部分序号（如 `1.2`、`1.3`）编号，落盘文件名 `1.2__invoice.pdf` |
| SQLite 记录 UIDVALIDITY/UID/进度 | `mailbox_epochs` / `messages` / `attachments` / `collection_runs` |
| 邮箱重建后从新 UID 空间重采 | UIDVALIDITY 变化即建立新代际（epoch），新旧代际独立，旧原件与来源仍可查询下载 |
| 附件全部落盘后索引与进度一起提交 | 先写 staging，整封原子 `rename` 发布，再在**单个 SQLite 事务**内写索引+进度 |
| 断流/落盘失败/进程退出不暴露半封 | 半封只存在于 `staging/`（API 只索引 `blobs/`）；失败 `finally` 清理，启动时清理崩溃遗留；该封不写进度 |
| 重启续采不重复 | 已落库邮件（done/oversize/failed）按 (UIDVALIDITY, UID) 跳过；断流/落盘失败等未落库邮件才重采；正式路径由 UIDVALIDITY/UID/部分序号确定，重试幂等 |
| 仅一个采集进程，并发不并行 | `fcntl` 排他文件锁（跨进程）+ 线程锁（同进程并发立即 409，不排队） |

## 目录结构

```
.
├── docker-compose.yml          # 一键启动 imap + 播种 + collector
├── collector/                  # FastAPI + SQLite 采集服务
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API 入口（/docs）
│       ├── collector.py        # 采集流水线与事务/故障边界
│       ├── imap_client.py      # 只读 IMAP（EXAMINE + UID + BODY.PEEK）
│       ├── pdf_extract.py      # MIME 遍历、PDF 提取、部分序号
│       ├── storage.py          # staging -> 原子发布 -> blobs
│       ├── database.py         # SQLite 持久层
│       ├── process_lock.py     # 单采集进程文件锁
│       └── config.py
├── imap_server/                # 本地 IMAP 演示服务（纯标准库 asyncio）
│   ├── Dockerfile
│   └── imapdemo/
│       ├── server.py           # IMAP4rev1 只读子集 + 断流注入
│       ├── state.py            # 邮箱状态（删信/重建/断流开关）
│       ├── seed.py             # 5 封演示邮件播种
│       └── control.py          # 邮箱控制 CLI
└── tools/demo.sh               # 端到端演示脚本
```

## 快速开始

```bash
docker compose up -d --build
# 浏览器打开
#   http://localhost:8000/docs      采集/进度/下载 Swagger 文档
#   IMAP 演示端口 127.0.0.1:1430
```

启动后 `imap-seed` 一次性任务会写入 5 封演示邮件：

| UID | 内容 | 预期 |
| --- | --- | --- |
| 1 | 单个 PDF | done，1 附件 |
| 2 | 两个**同名** PDF + 一个 txt | done，2 附件，序号 1.2 / 1.3 |
| 3 | 多附件大邮件（约 6.7 MiB） | 断流演示目标 |
| 4 | 约 8.3 MiB 超限邮件 | oversize，不拉正文 |
| 5 | 无附件纯文本 | done，0 附件 |

## API 入口（详见 /docs）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/collect` | 触发一轮采集；并发触发返回 `409` |
| GET  | `/api/progress` | 各 UIDVALIDITY 代际的采集进度 |
| GET  | `/api/epochs` | 邮箱代际列表（每次重建一条） |
| GET  | `/api/runs` | 历史采集运行记录（含 aborted 原因） |
| GET  | `/api/messages` | 邮件与附件来源列表，可按 `uidvalidity`/`status` 过滤 |
| GET  | `/api/messages/{id}` | 单封邮件及附件来源 |
| GET  | `/api/sources/{uidvalidity}/{uid}` | **按来源（UIDVALIDITY+UID）** 定位 |
| GET  | `/api/messages/attachments/{id}/download` | 下载单个 PDF 原件 |
| GET  | `/api/sources/{uidvalidity}/{uid}/download` | 按来源 ZIP 打包下载该封全部 PDF |

## 演示三个关键场景

### 1) 多附件中途断流

```bash
# 让 UID3 的正文 FETCH 发送一半字节后断开
docker compose exec imap python -m imapdemo.control --data-dir /maildata flaky 3
curl -X POST http://localhost:8000/api/collect     # aborted，UID3 不推进、无半封
docker compose exec collector ls -A /data/staging  # 为空
# 恢复后续采，UID3 完整入库且不重复
docker compose exec imap python -m imapdemo.control --data-dir /maildata flaky-off
curl -X POST http://localhost:8000/api/collect
```

### 2) 删除前信导致序号变化

```bash
docker compose exec imap python -m imapdemo.control --data-dir /maildata delete-before 1
# 序号前移但 UID 不变；采集以 UID 定位，已采集邮件幂等跳过
```

### 3) 邮箱重建（UIDVALIDITY 变化）

```bash
docker compose exec imap python -m imapdemo.control --data-dir /maildata rebuild
docker compose run --rm --entrypoint \
  "python -m imapdemo.seed --data-dir /maildata --rebuild" imap-seed
curl -X POST http://localhost:8000/api/collect
curl http://localhost:8000/api/progress   # 出现两个 UIDVALIDITY 代际，旧原件仍可下载
```

一键全流程演示：`bash tools/demo.sh`。

## 存储布局

```
/data
├── collect.db                     # SQLite（WAL）
├── collect.lock                   # 单采集进程排他锁
├── staging/tmp-<uuid>/            # 半封临时区，成功或失败后即清空
└── blobs/
    └── uidvalidity-<UIDVALIDITY>/
        └── uid-<UID>/
            ├── 1.2__invoice.pdf   # <MIME 部分序号>__<原文件名>
            └── 1.3__invoice.pdf
```

## 本地开发（无 Docker）

```bash
# 采集服务
pip install -r collector/requirements.txt
DATA_DIR=./_data IMAP_HOST=127.0.0.1 IMAP_PORT=1430 \
  uvicorn app.main:app --reload --app-dir collector

# IMAP 演示服务（纯标准库）
PYTHONPATH=imap_server MAIL_DATA_DIR=./_mail IMAP_BIND_PORT=1430 \
  python -m imapdemo
PYTHONPATH=imap_server python -m imapdemo.seed --data-dir ./_mail
```
