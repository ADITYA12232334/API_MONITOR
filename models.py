# models.py
from typing import List
from uuid import UUID, uuid4
from pydantic import BaseModel
from enum import Enum

class Gender(str, Enum):
    male = "male"
    female = "female"
    non_binary = "non_binary"

class Role(str, Enum):
    user = "user"
    admin = "admin"
    super_admin = "super_admin"

class User(BaseModel):
    id: UUID = uuid4()
    first_name: str
    last_name: str
    gender: Gender
    roles: List[Role]

    def to_dict(self):
        return{
            "id":str(self.id),
            "first_name":self.first_name,
            "last_name":self.last_name,
            "gender":self.gender.value,
            "roles":[role.value for role in self.roles]
        }
    
