"""演示数据播种：直接构造 RFC822 邮件并写入邮箱状态。

播种 5 封覆盖全部演示场景的邮件：
  UID 1  单 PDF 附件（普通发票）
  UID 2  嵌套 multipart + 两个同名 PDF（invoice.pdf 位于 1.2.1 与 1.2.2）
         另含一个非 PDF 的 text 附件（应被忽略）
  UID 3  多附件大邮件（约 5 MiB）——断流演示目标
  UID 4  超 8 MiB 的邮件（约 8.3 MiB）——应被标记 oversize、不拉正文
  UID 5  无附件纯文本邮件——done 但附件数为 0

用法：
  python -m imapdemo.seed --data-dir /maildata [--rebuild]
--rebuild 用于邮箱重建演示：先切换到全新 UIDVALIDITY/UID 空间再播种。
"""
from __future__ import annotations

import argparse
from email.message import EmailMessage
from email.utils import formatdate

INTERNALDATE = "12-Nov-2026 10:00:00 +0800"
DATE_HEADER = formatdate(1_794_000_000, usegmt=True)


def make_pdf(payload_bytes: bytes, title: str = "Demo PDF") -> bytes:
    """生成最小合法单页 PDF（PDF 1.4），内容由调用方控制大小。"""
    content = (
        b"BT /F1 14 Tf 50 780 Td (" + title.encode("ascii", "replace")
        + b") Tj ET\n" + payload_bytes
    )
    objects: list[bytes] = []
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
    )
    objects.append(
        b"<< /Length " + str(len(content)).encode() + b" >>\nstream\n"
        + content + b"\nendstream"
    )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{index} 0 obj\n".encode() + body + b"\nendobj\n"
    xref_pos = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        out += f"{off:010d} 00000 n \n".encode()
    out += (
        b"trailer\n<< /Size " + str(len(objects) + 1).encode()
        + b" /Root 1 0 R >>\nstartxref\n" + str(xref_pos).encode()
        + b"\n%%EOF\n"
    )
    return bytes(out)


def pad_pdf(target_size: int, title: str) -> bytes:
    filler_len = max(0, target_size - 400)
    filler = b"0.1 0.2 0.3 rg 10 10 500 700 re f\n" * (filler_len // 32 + 1)
    pdf = make_pdf(filler[:filler_len], title)
    # 若仍不足，用 PDF 注释补齐（不影响合法性）
    if len(pdf) < target_size:
        note = b"\n% " + b"x" * (target_size - len(pdf)) + b"\n%%EOF\n"
        pdf = pdf[:-6] + note
    return pdf


def base_message(subject: str, sender: str, recipient: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg["Date"] = DATE_HEADER
    # 故意设置 Message-ID：采集方不允许用它去重，仅入库备查
    msg["Message-ID"] = f"<{subject.replace(' ', '-').lower()}@vendor.example>"
    return msg


def build_messages() -> list[bytes]:
    mails: list[bytes] = []

    # ---- UID 1：单附件 ----
    m1 = base_message("Invoice INV-1001", "billing@vendor-a.example",
                      "invoicing@example.com")
    m1.set_content("请查收发票 INV-1001。")
    m1.add_attachment(
        make_pdf(b"", "INV-1001"), maintype="application", subtype="pdf",
        filename="invoice.pdf",
    )
    mails.append(m1.as_bytes())

    # ---- UID 2：根 multipart/mixed 直接含两个同名 PDF（按 MIME 序号 1.2/1.3 区分）
    # ---- 另含一个非 PDF 的 text 附件（应被忽略）
    m2 = base_message("Invoice INV-1002 two copies", "billing@vendor-b.example",
                      "invoicing@example.com")
    m2.set_content("两联发票（同名 PDF 两份），另附说明文本。")
    m2.add_attachment(
        make_pdf(b"copy-1", "INV-1002 copy 1"),
        maintype="application", subtype="pdf", filename="invoice.pdf",
    )
    m2.add_attachment(
        make_pdf(b"copy-2", "INV-1002 copy 2"),
        maintype="application", subtype="pdf", filename="invoice.pdf",
    )
    m2.add_attachment(
        b"this is a plain text note, not a pdf",
        maintype="text", subtype="plain", filename="note.txt",
    )
    mails.append(m2.as_bytes())

    # ---- UID 3：多附件大邮件（约 5 MiB），断流目标 ----
    m3 = base_message("Invoice INV-1003 large multi", "billing@vendor-c.example",
                      "invoicing@example.com")
    m3.set_content("大额发票，含两个 PDF 附件。")
    m3.add_attachment(
        pad_pdf(2_500_000, "INV-1003 part A"),
        maintype="application", subtype="pdf", filename="big-a.pdf",
    )
    m3.add_attachment(
        pad_pdf(2_500_000, "INV-1003 part B"),
        maintype="application", subtype="pdf", filename="big-b.pdf",
    )
    mails.append(m3.as_bytes())

    # ---- UID 4：超限邮件（约 8.3 MiB）----
    m4 = base_message("Invoice INV-1004 oversized", "billing@vendor-d.example",
                      "invoicing@example.com")
    m4.set_content("超限发票邮件。")
    m4.add_attachment(
        pad_pdf(8_300_000, "INV-1004 too big"),
        maintype="application", subtype="pdf", filename="oversized.pdf",
    )
    mails.append(m4.as_bytes())

    # ---- UID 5：无附件 ----
    m5 = base_message("Payment notice no attachment", "ar@vendor-e.example",
                      "invoicing@example.com")
    m5.set_content("本邮件无附件，仅用于测试无 PDF 场景。")
    mails.append(m5.as_bytes())

    return mails


def main() -> None:
    parser = argparse.ArgumentParser(description="播种演示邮件")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--rebuild", action="store_true",
                        help="先重建邮箱（新 UIDVALIDITY）再播种")
    args = parser.parse_args()

    from .state import MailState

    state = MailState(args.data_dir)
    if args.rebuild:
        snap = state.rebuild()
        print(f"邮箱已重建，新 UIDVALIDITY={snap['uidvalidity']}")
    for eml in build_messages():
        snap = state.append_message(eml, INTERNALDATE)
    snap = state.snapshot()
    print(
        f"播种完成：UIDVALIDITY={snap['uidvalidity']}，"
        f"共 {len(snap['messages'])} 封，下一 UID={snap['next_uid']}"
    )
    for msg in snap["messages"]:
        print(f"  UID {msg['uid']:>3}  {msg['size']:>10} 字节")


if __name__ == "__main__":
    main()
