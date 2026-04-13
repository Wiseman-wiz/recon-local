from typing import Any
import pymongo

class Connector:
    def __init__(self) -> None:
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")



    def conn(self) -> Any:
        pass