"""REPOSITORIES
Methods to interact with the database
"""

# # Package # #
from pydantic import BaseModel
from .models import *
from .exceptions import *
# from .database import collection

from .utils import get_time, get_uuid

__all__ = ("PeopleRepository", "collection")

from typing import List, Optional, Dict


class OurResult:
    def __init__(self, acknowledged: bool = False, inserted_id: Optional[str] = None):
        self.acknowledged: bool = acknowledged
        self.inserted_id: Optional[str] = inserted_id
        self.modified_count: int = 0
        self.deleted_count: int = 0


class collection:
    persons: Dict[str, Dict] = {}

    @classmethod
    def find_one(cls, person_id_dict: {}):
        if person_id_dict["_id"] in cls.persons:
            return cls.persons[person_id_dict["_id"]]
        else:
            return None

    @classmethod
    def find(cls):
        ret_val: List[Dict] = []
        for obj in cls.persons.values():
            ret_val.append(obj)
        return ret_val

    @classmethod
    def insert_one(cls, in_dict: Dict):
        ret_val: OurResult = OurResult()
        if "_id" in in_dict:
            p_id = in_dict["_id"]
            if p_id not in cls.persons:
                cls.persons[p_id] = in_dict
                ret_val.acknowledged = True
                ret_val.inserted_id = p_id
        return ret_val

    @classmethod
    def update_one(cls, id_dict: Dict, field_dict: Dict):
        ret_val: OurResult = OurResult()
        if "_id" in id_dict:
            p_id = id_dict["_id"]
            if p_id not in cls.persons:
                if "$set" in field_dict:
                    cls.persons[p_id].update(field_dict["$set"])
                    ret_val.acknowledged = True
                    ret_val.modified_count = 1  # not actual count - but makes it work
                    ret_val.inserted_id = p_id
        return ret_val

    @classmethod
    def delete_one(cls, id_dict: Dict):
        ret_val: OurResult = OurResult()
        if "_id" in id_dict:
            p_id = id_dict["_id"]
            if p_id not in cls.persons:
                del cls.persons[p_id]
                ret_val.acknowledged = True
                ret_val.deleted_count = 1  # not actual count - but makes it work
                ret_val.inserted_id = p_id
        return ret_val

    @classmethod
    def delete_many(cls, id_dict: Dict):
        cls.persons.clear()
        return


class PeopleRepository:
    @staticmethod
    def get(person_id: str) -> PersonRead:
        """Retrieve a single Person by its unique id"""
        document = collection.find_one({"_id": person_id})
        if not document:
            raise PersonNotFoundException(person_id)
        return PersonRead(**document)

    @staticmethod
    def list() -> PeopleRead:
        """Retrieve all the available persons"""
        cursor = collection.find()
        return [PersonRead(**document) for document in cursor]

    @staticmethod
    def create(create: PersonCreate) -> PersonRead:
        """Create a person and return its Read object"""
        document = create.dict()
        document["created"] = document["updated"] = get_time()
        document["_id"] = get_uuid()
        # The time and id could be inserted as a model's Field default factory,
        # but would require having another model for Repository only to implement it

        result = collection.insert_one(document)
        assert result.acknowledged

        return PeopleRepository.get(result.inserted_id)

    @staticmethod
    def update(person_id: str, update: PersonUpdate):
        """Update a person by giving only the fields to update"""
        document = update.dict()
        document["updated"] = get_time()

        result = collection.update_one({"_id": person_id}, {"$set": document})
        if not result.modified_count:
            raise PersonNotFoundException(identifier=person_id)

    @staticmethod
    def delete(person_id: str):
        """Delete a person given its unique id"""
        result = collection.delete_one({"_id": person_id})
        if not result.deleted_count:
            raise PersonNotFoundException(identifier=person_id)
