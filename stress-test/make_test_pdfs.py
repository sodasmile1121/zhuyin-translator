"""Generate the document-size set used by the k6 scripts, into ./pdfs/ (git-ignored).

    python3 make_test_pdfs.py

Sizes (Han characters are what the worker's cost scales with, see the table printed at the end):
    test.pdf (tracked)  1 page,   about 15 characters
    doc_s.pdf           3 pages,  about 300
    doc_m.pdf          10 pages,  about 6,000
    doc_l.pdf          30 pages,  about 22,000

Run a benchmark against one size:
    k6 run -e PDF_FILE=./pdfs/doc_m.pdf -e VUS=20 -e TARGET_URL=http://<host>:8081 mixed_lifecycle_benchmark.js
"""
import os
import re

import fitz
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

HERE = os.path.dirname(os.path.abspath(__file__))
FONT = os.path.join(HERE, "..", "python-worker", "fonts", "NotoSansTC-VariableFont_wght.ttf")
OUT = os.path.join(HERE, "pdfs")
LINES = [
    "今天天氣很好，我們一起去公園玩耍，看到許多小朋友正在放風箏。",
    "老師說，學習語文要多讀多寫，每天讀一點書，就會慢慢進步。",
    "媽媽在廚房煮飯，爸爸在客廳看報紙，弟弟在房間裡畫圖。",
]
SIZES = {"doc_s": (3, 4), "doc_m": (10, 25), "doc_l": (30, 30)}  # name: (pages, lines per page)


def make_pdf(path, pages, lines_per_page):
    c = canvas.Canvas(path)
    for page in range(pages):
        c.setFont("TC", 14)
        y = 780
        for i in range(lines_per_page):
            c.drawString(50, y, LINES[(page + i) % len(LINES)])
            y -= 24
        c.showPage()
    c.save()


def describe(path):
    with fitz.open(path) as doc:
        han = sum(len(re.findall(r"[一-鿿]", page.get_text())) for page in doc)
        return len(doc), han


if __name__ == "__main__":
    pdfmetrics.registerFont(TTFont("TC", FONT))
    os.makedirs(OUT, exist_ok=True)
    for name, (pages, lines_per_page) in SIZES.items():
        make_pdf(os.path.join(OUT, name + ".pdf"), pages, lines_per_page)

    print(f"{'file':22} {'pages':>5} {'Han chars':>10} {'size KB':>8}")
    for label, path in [("test.pdf", os.path.join(HERE, "test.pdf"))] + [
        (f"pdfs/{n}.pdf", os.path.join(OUT, n + ".pdf")) for n in SIZES
    ]:
        pages, han = describe(path)
        print(f"{label:22} {pages:>5} {han:>10} {os.path.getsize(path) / 1024:>8.1f}")
