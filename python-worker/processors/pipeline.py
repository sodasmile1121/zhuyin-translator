from services.pdf_service import extract_text_from_pdf
from services.nlp_service import get_pinyin
from models.document import Document


def process_file_task(path):
    doc = Document()
    doc.text = extract_text_from_pdf(path)
    doc.char_zy = get_pinyin(doc.text)
    
    return {
        "status": "success",
        "path": path,
        "preview": doc.text[0][:10],
        "char_zy": doc.char_zy[0][:10],
    }