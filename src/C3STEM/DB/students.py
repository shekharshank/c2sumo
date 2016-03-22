import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

connection = MongoClient()
db = connection.c3stem_database

db.student.insert({
    "_id": "andy",
    "group_id": "group_A",
    "password": "password",
    "firstname": "Andy",
    "lastname": "Gokhale",
})

db.user_studentrole.insert({
    "user_id": "andy",
    "role_ids": ["user", "admin"]
})

db.student.insert({
    "_id": "sneezy",
    "group_id": "group_A",
    "password": "password",
    "firstname": "sneezy",
    "lastname": "smith",
})

db.user_studentrole.insert({
    "user_id": "sneezy",
    "role_ids": ["user", "admin"]
})

db.student.insert({
    "_id": "sleepy",
    "group_id": "group_A",
    "password": "password",
    "firstname": "sleepy",
    "lastname": "smith",
})

db.user_studentrole.insert({
    "user_id": "sleepy",
    "role_ids": ["user", "admin"]
})

db.student.insert({
    "_id": "ipad",
    "group_id": "group_A",
    "password": "password",
    "firstname": "Ipad",
    "lastname": "User",
})

db.user_studentrole.insert({
    "user_id": "ipad",
    "role_ids": ["user", "admin"]
})

db.student.insert({
    "_id": "nexus",
    "group_id": "group_A",
    "password": "password",
    "firstname": "Nexus",
    "lastname": "User",
})

db.user_studentrole.insert({
    "user_id": "nexus",
    "role_ids": ["user", "admin"]
})
