import os
import tempfile
import unicodedata
import unittest

os.environ.setdefault("REDIS_ADDR", "localhost:6379")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_S3_BUCKET", "test-bucket")
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", tempfile.mkdtemp())

import fitz
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

import worker
from processors.pipeline import process_file_task

FONT_PATH = os.path.join(os.path.dirname(__file__), "fonts", "NotoSansTC-VariableFont_wght.ttf")
LINES = [
    "今天天氣很好，我們一起去公園玩耍，看到許多小朋友正在放風箏。",
    "老師說，學習語文要多讀多寫，每天讀一點書，就會慢慢進步。",
    "媽媽在廚房煮飯，爸爸在客廳看報紙，弟弟在房間裡畫圖。",
]
PAGES = 3


def is_han(ch):
    return 0x4E00 <= ord(ch) <= 0x9FFF


def han_count(text):
    return sum(1 for ch in text if is_han(ch))


def make_pdf(path, pages):
    pdfmetrics.registerFont(TTFont("TestTC", FONT_PATH))
    c = canvas.Canvas(path)
    for lines in pages:
        c.setFont("TestTC", 14)
        y = 780
        for line in lines:
            c.drawString(50, y, line)
            y -= 24
        c.showPage()
    c.save()


class TestMultiPageDocument(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.src = os.path.join(self.tmp.name, "input.pdf")
        pages = [[LINES[(page + i) % len(LINES)] for i in range(12)] for page in range(PAGES)]
        make_pdf(self.src, pages)
        with fitz.open(self.src) as doc:
            self.input_han = sum(han_count(page.get_text()) for page in doc)

    def test_output_pdf_contains_every_character_of_the_input(self):
        out = os.path.join(self.tmp.name, "output.pdf")
        worker.run_translation_pipeline(self.src, out)
        with fitz.open(out) as doc:
            output_han = sum(han_count(page.get_text()) for page in doc)
        self.assertEqual(output_han, self.input_han)

    def test_cached_result_keeps_only_a_short_preview(self):
        out = os.path.join(self.tmp.name, "output.pdf")
        result = worker.run_translation_pipeline(self.src, out)
        self.assertLessEqual(len(result["char_zy"]), 10)
        self.assertNotIn("full_char_zy", result)


class TestPinyinAlignment(unittest.TestCase):

    def test_mixed_content_keeps_zhuyin_aligned_with_characters(self):
        text = "我有3本書，ABC很好。"
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "mixed.pdf")
            make_pdf(src, [[text]])
            result = process_file_task(src)
        items = result["full_char_zy"]
        # clean_text() applies NFKC normalisation, e.g. the full-width comma becomes ","
        self.assertEqual("".join(item["char"] for item in items), unicodedata.normalize("NFKC", text))
        for item in items:
            if is_han(item["char"]):
                self.assertNotEqual(item["zy"], "", item["char"])
            else:
                self.assertEqual(item["zy"], "", item["char"])


if __name__ == "__main__":
    unittest.main()
