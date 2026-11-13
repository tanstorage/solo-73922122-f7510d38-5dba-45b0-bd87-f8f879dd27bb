#!/usr/bin/env bash
# 端到端演示脚本：多附件中途断流、重启续采、单进程并发保护、删信序号变化、邮箱重建。
# 前置：docker compose up -d --build
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
IMAP_CTL=(docker compose exec -T imap python -m imapdemo.control --data-dir /maildata)

line() { printf '\n==================== %s ====================\n' "$1"; }
api() { curl -sS "$BASE$1" | python3 -m json.tool; }
current_uv() { curl -sS "$BASE/api/progress" | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["uidvalidity"])'; }

line "0. 健康检查"
curl -sS "$BASE/health"; echo

line "1. 注入断流：UID 3（多附件大邮件）FETCH 正文发送一半即断开"
"${IMAP_CTL[@]}" flaky 3
"${IMAP_CTL[@]}" status

line "2. 首轮采集：UID 1/2 成功、UID 3 断流中止（不推进、无半封）、本轮 aborted"
curl -sS -X POST "$BASE/api/collect" | python3 -m json.tool

line "3. 进度与来源：UID 3 无记录；staging 为空；blobs 下没有 uid-3 目录"
api "/api/progress"
api "/api/messages"
docker compose exec -T collector sh -c 'echo "--- staging（应为空）---"; ls -A /data/staging; echo "--- blobs ---"; find /data/blobs -maxdepth 3 -type d | sort'

line "4. 单采集进程保护：在采集容器内持有同一把锁，API 触发必须返回 409"
# 后台持锁 8 秒（真实代码路径 ProcessLock），制造"已有采集进行中"
docker compose exec -d collector python -c \
  "import time; from app.process_lock import ProcessLock; from app.config import load_settings; \
l=ProcessLock(load_settings().lock_path); l.acquire_nonblocking(); time.sleep(8)"
sleep 1
curl -sS -o /tmp/collect-busy.json -w "持锁期间触发 HTTP 状态码: %{http_code}\n" -X POST "$BASE/api/collect"
cat /tmp/collect-busy.json | python3 -m json.tool
echo "等待锁释放..."
sleep 8
curl -sS -o /dev/null -w "锁释放后触发 HTTP 状态码: %{http_code}\n" -X POST "$BASE/api/collect"

line "5. 关闭断流，重新采集：重启续采成功，UID 3 附件完整且不重复"
"${IMAP_CTL[@]}" flaky-off
curl -sS -X POST "$BASE/api/collect" | python3 -m json.tool
UV="$(current_uv)"
api "/api/sources/$UV/3"

line "6. 删除最早一封：序号前移但 UID 不变，再采集只跳过终态邮件"
"${IMAP_CTL[@]}" delete-before 1
"${IMAP_CTL[@]}" status
curl -sS -X POST "$BASE/api/collect" | python3 -m json.tool

line "7. 邮箱重建：control 切换到新 UIDVALIDITY 空 UID 空间后重新播种"
"${IMAP_CTL[@]}" rebuild
docker compose run --rm -T --entrypoint "python -m imapdemo.seed --data-dir /maildata" imap-seed
curl -sS -X POST "$BASE/api/collect" | python3 -m json.tool
line "重建后进度：两个 UIDVALIDITY 代际并存"
api "/api/progress"
line "旧代际原件与来源仍可查询、下载"
api "/api/messages"

line "8. 下载演示（单附件 + 按来源 ZIP 打包）"
ATT_ID=$(curl -sS "$BASE/api/messages" | python3 -c 'import json,sys
for m in json.load(sys.stdin):
    if m["attachments"]:
        print(m["attachments"][0]["id"]); break')
curl -sS -o /tmp/demo-invoice.pdf -w "单附件下载 HTTP: %{http_code}, 字节: %{size_download}\n" \
  "$BASE/api/messages/attachments/$ATT_ID/download"
file /tmp/demo-invoice.pdf || true
NEW_UV="$(current_uv)"
curl -sS -o /tmp/source-bundle.zip -w "来源 ZIP 下载 HTTP: %{http_code}, 字节: %{size_download}\n" \
  "$BASE/api/sources/$NEW_UV/2/download"
python3 -c 'import zipfile; print("ZIP 内容:", zipfile.ZipFile("/tmp/source-bundle.zip").namelist())'

line "完成。浏览器打开 $BASE/docs 可在线试用全部 API"
