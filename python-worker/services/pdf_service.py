import os
import re
import unicodedata
import fitz
from typing import List


def extract_text_from_pdf(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Cannot find the file: {file_path}")

    with fitz.open(file_path) as doc:
        res = [clean_text(page.get_text()) for page in doc]
    return res


def clean_text(text: str) -> str:
    text = text.replace('\n', '')
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    text = re.sub(r'\s+', '', text)
    return unicodedata.normalize('NFKC', text)