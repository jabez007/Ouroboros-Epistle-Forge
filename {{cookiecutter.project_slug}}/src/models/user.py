from typing import List, Optional

from pydantic import BaseModel


class User(BaseModel):
    id: int
    name: str
    email: str
    active: bool = True
    tags: Optional[List[str]] = None
