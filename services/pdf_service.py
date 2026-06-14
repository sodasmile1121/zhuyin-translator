import os
import re
import unicodedata
import fitz
import shutil
import uuid
from typing import List
from fastapi import UploadFile


def save_files(files: list[UploadFile]):
    dir = "./uploads"
    if not os.path.exists(dir):
        os.makedirs(dir)
    res = []
    for file in files:
        file.file.seek(0)  # make sure copy every file from the beginning
        path = os.path.join(dir, f"{str(uuid.uuid4())}_{file.filename}")
        with open(path, 'wb') as fdst:
            shutil.copyfileobj(file.file, fdst)
        res.append(path)
    return res

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