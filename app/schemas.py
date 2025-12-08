# app/schemas.py
from pydantic import BaseModel
from typing import Any, List

class PagingResponse(BaseModel):
    count: int
    offset: int
    limit: int
    data: List[Any]
