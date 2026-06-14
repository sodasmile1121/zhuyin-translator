from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import Annotated
from services.pdf_service import save_files
from processors.pipeline import process_file_task
from api import schemas
import asyncio


router = APIRouter()

@router.post("/uploadfiles/", response_model=schemas.FinalResponse)
async def create_upload_files(
    files: Annotated[list[UploadFile], File()]
):
    check_file_type(files)
    paths = save_files(files)

    tasks = [process_file_task(p) for p in paths]
    result = await asyncio.gather(*tasks)

    return {"data": result}


def check_file_type(files: list[UploadFile]):
    for file in files:
        if file.content_type != "application/pdf":
            raise HTTPException(
                status_code=400,
                detail=f"{file.filename} is not a pdf file"
            )