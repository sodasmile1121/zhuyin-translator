from dataclasses import dataclass

@dataclass
class PDFJob:
    job_id: str = ""
    file_path: str = ""