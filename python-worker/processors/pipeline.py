from services.pdf_service import extract_text_from_pdf
from services.nlp_service import get_pinyin
from models.document import Document


PREVIEW_CHARS = 10


def process_file_task(path):
    doc = Document()
    doc.text = extract_text_from_pdf(path)
    doc.char_zy = get_pinyin(doc.text)

    # "char_zy" is only a preview for the cache and the frontend;
    # "full_char_zy" holds every page and is what the output PDF is rendered from.
    return {
        "status": "success",
        "path": path,
        "preview": doc.text[0][:PREVIEW_CHARS],
        "char_zy": doc.char_zy[0][:PREVIEW_CHARS],
        "full_char_zy": [item for page in doc.char_zy for item in page],
    }