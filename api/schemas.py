from pydantic import BaseModel
from typing import List

class CharZyPair(BaseModel):
    char: str
    zy: str

class TaskResult(BaseModel):
    path: str
    preview: str
    char_zy: List[CharZyPair]

class FinalResponse(BaseModel):
    data: List[TaskResult]