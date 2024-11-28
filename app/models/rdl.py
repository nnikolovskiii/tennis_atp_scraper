from pydantic import BaseModel

from app.databases.mongo_database.mongo_database import MongoEntry


class Player(BaseModel):
    id: str
    name: str
    url: str

class Statistic(MongoEntry):
    explanation: str
