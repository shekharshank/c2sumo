import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

connection = MongoClient()
db = connection.c3stem_database

db.studentgroup.insert({
    "_id": "group_C",
    "name": "Group C",
    "latitude": "36.138099",
    "longitude": "-86.806004"    
})

user_id = db.student.insert({
    "_id": "super.admin",
    "group_id": "group_C",
    "password": "password"
})

db.user_studentrole.insert({
    "user_id": "super.admin",
    "role_ids": ["super"]
})

colab_id = db.simulation.insert({
    "group_id": "group_C",
    "duration": 1000,
    #"starttime":"",
    #"endtime":"",
    "junctions":['202305472', '202305458'],
    "latitude": "36.138099",
    "longitude": "-86.806004",
    "update_rate": 4,
    "mode": "colab"
})

# A, ind
db.simulation_association.insert({
    "sim_id" : ObjectId("51c320cf0e9f2d2fcd195095"),
    "sim_asso" : [ObjectId("51c320cf0e9f2d2fcd195095")]
})

# B, ind
db.simulation_association.insert({
    "sim_id" : ObjectId("51c320cf0e9f2d2fcd1950b9"),
    "sim_asso" : [ObjectId("51c320cf0e9f2d2fcd1950b9")]
})

# A, colab
db.simulation_association.insert({
    "sim_id" : ObjectId("51c320cf0e9f2d2fcd1950a7"),
    "sim_asso" : [ObjectId("51c320cf0e9f2d2fcd1950a7")]
})

# B, colab
db.simulation_association.insert({
    "sim_id" : ObjectId("51c320cf0e9f2d2fcd1950cb"),
    "sim_asso" : [ObjectId("51c320cf0e9f2d2fcd1950cb")]
})

db.simulation_association.insert({
    "sim_id" : colab_id,
    "sim_asso" : [ObjectId("51c320cf0e9f2d2fcd1950a7"), ObjectId("51c320cf0e9f2d2fcd1950cb")]
})

