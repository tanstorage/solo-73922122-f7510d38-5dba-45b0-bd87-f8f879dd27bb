"""邮箱演示控制 CLI。

用法（容器内 / 本地均可）：
  python -m imapdemo.control --data-dir /maildata status
  python -m imapdemo.control --data-dir /maildata delete-before 1
  python -m imapdemo.control --data-dir /maildata flaky 3
  python -m imapdemo.control --data-dir /maildata flaky-off
  python -m imapdemo.control --data-dir /maildata rebuild
"""
from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="本地 IMAP 演示邮箱控制")
    parser.add_argument("--data-dir", required=True)
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("status", help="查看 UIDVALIDITY、UID 列表与断流设置")
    p_del = sub.add_parser("delete-before", help="删除最早的 N 封（序号变化演示）")
    p_del.add_argument("count", type=int)

    p_flaky = sub.add_parser("flaky", help="设置断流 UID（FETCH 中途断开）")
    p_flaky.add_argument("uid", type=int)
    sub.add_parser("flaky-off", help="取消断流设置")
    sub.add_parser("rebuild", help="邮箱重建：切换到新 UIDVALIDITY 空 UID 空间")

    args = parser.parse_args()
    from .state import MailState

    state = MailState(args.data_dir)

    if args.action == "status":
        snap = state.snapshot()
        print(f"UIDVALIDITY = {snap['uidvalidity']}")
        print(f"next_uid    = {snap['next_uid']}")
        print(f"flaky_uid   = {snap['flaky_uid']}")
        print("messages（序号:UID = 字节数）:")
        for seq, msg in enumerate(snap["messages"], start=1):
            mark = " [FLAKY]" if snap["flaky_uid"] == msg["uid"] else ""
            print(f"  #{seq:<3} UID {msg['uid']:<4} = {msg['size']:>10} 字节{mark}")
    elif args.action == "delete-before":
        snap = state.delete_before(args.count)
        print(f"已删除最早 {args.count} 封，剩余 {len(snap['messages'])} 封；"
              f"存活邮件 UID 保持不变：{[m['uid'] for m in snap['messages']]}")
    elif args.action == "flaky":
        snap = state.set_flaky(args.uid)
        print(f"已设置断流：UID {args.uid} 的 BODY FETCH 将发送一半后断开")
    elif args.action == "flaky-off":
        snap = state.set_flaky(None)
        print("已取消断流设置")
    elif args.action == "rebuild":
        snap = state.rebuild()
        print(f"邮箱已重建：新 UIDVALIDITY={snap['uidvalidity']}，"
              f"UID 空间重置，next_uid=1；旧 eml 文件与采集原件均保留")


if __name__ == "__main__":
    main()
