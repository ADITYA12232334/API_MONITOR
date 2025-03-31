# main.py
from typing import List,Optional
from uuid import uuid4, UUID
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from models import Gender, Role, User
from kafka_producer import create_kafka_producer,delivery_report,produce_messages
from workload_simulator import WorkloadSimulator
from confluent_kafka import Producer
import json
import time
import requests
import random
import threading
import logging
import sys
import uuid


app = FastAPI()
@app.get("/")
async def root():
 bootstrap_server="localhost:9092"
 topic="hello_topic"
 producer=create_kafka_producer(bootstrap_server)
 
 message={"greeting":"Hello world"}
 produce_messages(producer,topic,message)

 return message
db: List[User] = [
    User(
        id=uuid4(),
        first_name="John",
        last_name="Doe",
        gender=Gender.male,
        roles=[Role.user],
    ),
    User(
        id=uuid4(),
        first_name="Jane",
        last_name="Doe",
        gender=Gender.female,
        roles=[Role.user],
    ),
    User(
        id=uuid4(),
        first_name="James",
        last_name="Gabriel",
        gender=Gender.male,
        roles=[Role.user],
    ),
    User(
        id=uuid4(),
        first_name="Eunit",
        last_name="Eunit",
        gender=Gender.male,
        roles=[Role.admin, Role.user],
    ),
]

class UserRequest(BaseModel):
    first_name: str
    last_name: str
    gender: Gender
    roles: List[Role]

@app.get("/api/v1/users")
async def get_users():
    bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
    topic = 'example_topic'
    
    # Create producer
    db_jsoniable=[user.to_dict() for user in db]
    producer=create_kafka_producer(bootstrap_servers)
    produce_messages(producer, topic, db_jsoniable)
    return db

@app.post("/api/v1/users")
async def create_user(user: User):
 db.append(user)
 return {"id": user.id}

@app.delete("/api/v1/users/{id}")
async def delete_user(id:UUID):
    for user in db:
        if user.id==id:
            db.remove(user)
            return
        raise HTTPException(
            status_code=404, detail=f"Delete user failed, id {id} not found."

        )
        
class UpdateUser(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] =None
    roles: Optional[List[Role]] =None


@app.put("/api/v1/users/{id}", response_model=UUID)
async def update_user(user_update: UpdateUser, id: UUID) -> UUID:
    """
    Update a user's information by ID.
    
    Args:
        user_update (UpdateUser): Partial user update information
        id (UUID): User's unique identifier
    
    Returns:
        UUID: The ID of the updated user
    
    Raises:
        HTTPException: 404 error if user is not found
    """
    for index, user in enumerate(db):
        if user.id == id:
            # Create a copy of the existing user to update
            updated_user = user.model_copy()
            
            # Apply updates only for non-None fields
            if user_update.first_name is not None:
                updated_user.first_name = user_update.first_name
            
            if user_update.last_name is not None:
                updated_user.last_name = user_update.last_name
            
            if user_update.roles is not None:
                updated_user.roles = user_update.roles
            
            # Replace the user in the database
            db[index] = updated_user
            
            return user.id
    
    # Raise 404 if no user found
    raise HTTPException(status_code=404, detail=f"Could not find user with id: {id}")


