from bson import ObjectId
from pydantic import BaseModel
from typing import Optional

from app.databases.mongo_database.mongo_database import MongoEntry


class Group(BaseModel):
    id: str
    context: str
    summed_group_id: Optional[str] = None
    level: Optional[int] = 0


class Type(BaseModel):
    id: Optional[str] = None
    type: str
    value: str
    description: str
    examples: Optional[str] = None
    parent_type: Optional[str] = None

    def __str__(self):
        return f"{self.value}: {self.description}"


class Document(BaseModel):
    id: str
    name: Optional[str] = None
    context: str


class Chunk(BaseModel):
    id: str
    doc_id: str
    context: str

class PlainText(MongoEntry):
    text: str