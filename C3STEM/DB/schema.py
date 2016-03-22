import pymongo
from pymongo import MongoClient
from datetime import datetime
import sys
import bcrypt
sys.path.insert(0, '/opt/C3STEM/Middleware')
from DAO import TrafficLightDAO

connection = MongoClient()
db = connection.c3stem_database

db.studentgroup.insert({
    "_id": "group_A",
    "name": "A",
    "latitude": "35.0419",
    "longitude": "-85.290953",
	"collaboration_url": "",
	"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
	"last_update_time": str(datetime.now())
})

db.studentgroup.insert({
    "_id": "group_B",
    "name": "B",
    "latitude": "35.038966",
    "longitude": "-85.282874",
	"collaboration_url": "",
	"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
	"last_update_time": str(datetime.now())
})

db.studentgroup.insert({
    "_id": "group_C",
    "name": "Admin",
    "latitude": "36.138099",
    "longitude": "-86.806004",
	"collaboration_url": "",
	"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000"    ,
	"last_update_time": str(datetime.now())
})

db.studentrole.insert({
    "_id": "user",
    "name": "User"
})

db.studentrole.insert({
    "_id": "admin",
    "name": "Admin"
})

db.studentrole.insert({
    "_id": "super",
    "name": "Super Admin"
})

hashed_pass = bcrypt.hashpw("password".encode('utf-8'), bcrypt.gensalt())

db.student.insert({
    "_id": "user_a",
    "firstname": "FirstA",
    "lastname": "LastA",
    "schoolname": "School X",
    "schoolcode": "schoolx",
    "group_id": "group_A",
    "password": hashed_pass,
    "changepassword": "true"
})

db.user_studentrole.insert({
    "user_id": "user_a",
    "role_ids": ["user","admin"]
})

user_id = db.student.insert({
    "_id": "user_b",
    "firstname": "FirstB",
    "lastname": "LastB",
    "schoolname": "School X",
    "schoolcode": "schoolx",
    "group_id": "group_A",
    "password": hashed_pass,
    "changepassword": "true"
})

db.user_studentrole.insert({
    "user_id": "user_b",
    "role_ids": ["user"]
})

user_id = db.student.insert({
    "_id": "user_c",
    "firstname": "FirstC",
    "lastname": "LastC",
    "schoolname": "School Y",
    "schoolcode": "schooly",
    "group_id": "group_A",
    "password": hashed_pass ,
    "changepassword": "true"
})

db.user_studentrole.insert({
    "user_id": "user_c",
    "role_ids": ["user"]
})

user_id = db.student.insert({
    "_id": "user_d",
    "firstname": "FirstD",
    "lastname": "LastD",
    "schoolname": "School Y",
    "schoolcode": "schooly",
    "group_id": "group_B",
    "password": hashed_pass,
    "changepassword": "true"
})

db.user_studentrole.insert({
    "user_id": "user_d",
    "role_ids": ["user", "admin"]
})

user_id = db.student.insert({
    "_id": "super.admin",
    "group_id": "group_C",
    "password": "password"
})

db.user_studentrole.insert({
    "user_id": "super.admin",
    "role_ids": ["user", "admin", "super"],
    "user_mode": "COLAB"
})

user_id = db.student.insert({
    "_id": "test",
    "group_id": "group_A",
    "password": "isfortesting"
})

db.user_studentrole.insert({
    "user_id": "test",
    "role_ids": ["user", "admin"]
})

user_id = db.student.insert({
    "_id": "student1",
    "group_id": "group_B",
    "password": "isastudent"
})

db.user_studentrole.insert({
    "user_id": "student1",
    "role_ids": ["user", "admin"]
})

user_id = db.student.insert({
    "_id": "student2",
    "group_id": "group_A",
    "password": "isastudent"
})

db.user_studentrole.insert({
    "user_id": "student2",
    "role_ids": ["user", "admin"]
})

user_id = db.student.insert({
    "_id": "student3",
    "group_id": "group_B",
    "password": "isastudent"
})

db.user_studentrole.insert({
    "user_id": "student3",
    "role_ids": ["user", "admin"]
})


######### create junctions and associate with groups

junction_id = db.junction.insert({
    "_id": "202305472",
    "west_lane_out":"-19485812#3",
    "west_lane_out_values":["-19485812#3_0","-19485812#3_1"],
    "west_lane_in":"19485812#3",
    "west_lane_in_values":["19485812#3_1", "19485812#3_0"],
    # left, straight, right
    "west_lane_in_adjascent":["-19457616#3", "-19456179#4", "19457616#4"],
    
    "east_lane_in":"19456179#4",
    "east_lane_in_values":["19456179#4_0","19456179#4_1"],
    "east_lane_in_adjascent":["19457616#4", "-19485812#3", "-19457616#3"],
    "east_lane_out":"-19456179#4",
    "east_lane_out_values":["-19456179#4_1", "-19456179#4_0"],
    
    "north_lane_in":"19457616#3",
    "north_lane_in_values":["19457616#3_0","19457616#3_1"],
    "north_lane_in_adjascent":["-19456179#4", "19457616#4", "-19485812#3"],
    "north_lane_out":"-19457616#3",
    "north_lane_out_values":["-19457616#3_1","-19457616#3_0"],
    
    "south_lane_out":"19457616#4",
    "south_lane_out_values":["19457616#4_0","19457616#4_1"],
    "south_lane_in":"-19457616#4",
    "south_lane_in_values":["-19457616#4_1", "-19457616#4_0"],
    "south_lane_in_adjascent":["-19485812#3", "-19457616#3", "-19456179#4"]
})

db.inductionloop.insert({
    "_id": "-19485812#3_0_5",
    "junction": junction_id,    
    "location": "west_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19485812#3_1_5",
    "junction": junction_id,    
    "location": "west_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19485812#3_0_-5",
    "junction": junction_id,    
    "location": "west_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19485812#3_1_-5",
    "junction": junction_id,    
    "location": "west_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19456179#4_0_5",
    "junction": junction_id,    
    "location": "east_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19456179#4_1_5",
    "junction": junction_id,    
    "location": "east_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19456179#4_0_-5",
    "junction": junction_id,    
    "location": "east_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19456179#4_1_-5",
    "junction": junction_id,    
    "location": "east_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19457616#3_0_5",
    "junction": junction_id,    
    "location": "north_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19457616#3_1_5",
    "junction": junction_id,    
    "location": "north_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19457616#3_0_-5",
    "junction": junction_id,    
    "location": "north_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19457616#3_1_-5",
    "junction": junction_id,    
    "location": "north_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19457616#4_0_5",
    "junction": junction_id,    
    "location": "south_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19457616#4_1_5",
    "junction": junction_id,    
    "location": "south_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19457616#4_0_-5",
    "junction": junction_id,    
    "location": "south_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19457616#4_1_-5",
    "junction": junction_id,    
    "location": "south_lane_in",
    "pos": -5
})   


colab_id = db.simulation.insert({
    "group_id": "group_C",
    "duration": 1000,
    #"starttime":"",
    #"endtime":"",
    "junctions":['202305472', '202305458'],
    "latitude": "36.138099",
    "longitude": "-86.806004",
    "update_rate": 2,
    "max_update_size": 50,
    "mode": "COLAB",
    "problem_id":"1",
    "status": "ACTIVE"
})    


simulation_id = db.simulation.insert({
    "group_id": "group_A",
    "duration": 1000,
    #"starttime":"",
    #"endtime":"",
    "junctions":[junction_id],
    "latitude": "36.138724",
    "longitude": "-86.810884",
    "update_rate": 2,
    "max_update_size": 50,
    "mode": "GROUP",
    "problem_id":"1",
    "status": "ACTIVE"
})

state = "Green!0g!1-Green!2!3-Green!4g!5!6-Green!7!8-Green!9g!10-Green!11!12-Green!13g!14!15-Green!16!17"
duration = 10
TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state, duration)

db.simulation_association.insert({
    "sim_id" : simulation_id,
    "sim_asso" : [simulation_id]
})



db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Car",
	"accel": "20",
	"decel": "30",
	"sigma": "1",
	"max_speed": "100",
	"length": "10",
	"probability": "0.5"
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Bus",
	"accel": "15",
	"decel": "25",
	"sigma": "1",
	"max_speed": "70",
	"length": "15",
	"probability": "0.3"
})

db.vehicle.insert({
	"name": "Truck",
	"simulation_id": simulation_id, 
	"accel": "10",
	"decel": "15",
	"sigma": "1",
	"max_speed": "50",
	"length": "20",
	"probability": "0.2"
})

# First intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19485812#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19457616#3",
	"to_edge_right": "19457616#4",
	"to_edge_straight": "-19456179#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19457616#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19456179#4",
	"to_edge_right": "-19485812#3",
	"to_edge_straight": "19457616#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19456179#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "19457616#4",
	"to_edge_right": "-19457616#3",
	"to_edge_straight": "-19485812#3"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "-19457616#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19485812#3",
	"to_edge_right": "-19456179#4",
	"to_edge_straight": "-19457616#3"
})


# Second Intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19456179#0",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19463160#7",
	"to_edge_right": "19463160#8",
	"to_edge_straight": "-19479801#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19463160#7",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19479801#4",
	"to_edge_right": "19456179#0",
	"to_edge_straight": "19463160#8"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19479801#4",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19463160#8",
	"to_edge_right": "-19463160#7",
	"to_edge_straight": "19456179#0"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19463160#8",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19456179#0",
	"to_edge_right": "-19479801#4",
	"to_edge_straight": "-19463160#7"
})



#		| B					| C
#		| 					| 
#		| 		Iwest		|
# --A-- O1-----------------	O2---D---
#		|		Ieast		|
#		|					|
# Lsouth| Lnorth	Jsouth	| Jnorth
#		|					|
#		|		Keast		|
# --H-- O4----------------- O3---E---
#		|		Kwest		|
#		|					|
#		| G					| F

db.flows.insert({
    "point_name": "A",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "19485812#3",
	"to_edge_id": "n/a",
	"via_edge_id": "19485812#3",
    "flow_rate": "600",
	"latitude": "36.139691",
	"longitude": "-86.813",
	"removable": "0"
})

db.flows.insert({
    "point_name": "B",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "19457616#3",
	"to_edge_id": "n/a",
	"via_edge_id": "19457616#3",
    "flow_rate": "600",
	"latitude": "36.139572",
	"longitude": "-86.810",
	"removable": "0"
})

db.flows.insert({
    "point_name": "Iwest",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "19456179#4",
	"to_edge_id": "n/a",
	"via_edge_id": "19456179#4",
    "flow_rate": "600",
	"latitude": "36.138467",
	"longitude": "-86.808",
	"removable": "0"
})

db.flows.insert({
    "point_name": "Ieast",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "-19456179#0",
	"to_edge_id": "n/a",
	"via_edge_id": "-19456179#0",
    "flow_rate": "600",
	"latitude": "36.137900",
	"longitude": "-86.803260",
	"removable": "1"
})

db.flows.insert({
    "point_name": "Lnorth",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "-19457616#4",
	"to_edge_id": "n/a",
	"via_edge_id": "-19457616#4",
    "flow_rate": "600",
	"latitude": "36.138208",
	"longitude": "-86.810903",
	"removable": "0"
})

db.flows.insert({
    "point_name": "Lsouth",
	"simulation_id": simulation_id,
    "intersection_id": "202305472",
	"from_edge_id": "-19457616#4",
	"to_edge_id": "n/a",
	"via_edge_id": "-19457616#4",
    "flow_rate": "600",
	"latitude": "36.138266",
	"longitude": "-86.810977",
	"removable": "1"
})


''' This is just to see the 
schema of the table
db.trafficlightlogic.insert({
	"intersection_id": "202305472",
	"light_index": "0",
	"state": "Green",
	"duration": "30",
	"creation_time": datetime.now()
})
'''

simulation_id = db.simulation.insert({
    "group_id": "group_A",
    "duration": 1000,
    #"starttime":"",
    #"endtime":"",
    "junctions":[junction_id],
    "latitude": "36.138724",
    "longitude": "-86.810884",
    "update_rate": 2,
    "max_update_size": 50,
    "mode": "COLAB",
    "problem_id":"1",
    "status": "ACTIVE"
})
TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state, duration)

colab_id1 = simulation_id

db.simulation_association.insert({
    "sim_id" : simulation_id,
    "sim_asso" : [simulation_id]
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Car",
	"accel": "20",
	"decel": "30",
	"sigma": "1",
	"max_speed": "100",
	"length": "10",
	"probability": "0.5"
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Bus",
	"accel": "15",
	"decel": "25",
	"sigma": "1",
	"max_speed": "70",
	"length": "15",
	"probability": "0.3"
})

db.vehicle.insert({
	"name": "Truck",
	"simulation_id": simulation_id, 
	"accel": "10",
	"decel": "15",
	"sigma": "1",
	"max_speed": "50",
	"length": "20",
	"probability": "0.2"
})

# First intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19485812#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19457616#3",
	"to_edge_right": "19457616#4",
	"to_edge_straight": "-19456179#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19457616#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19456179#4",
	"to_edge_right": "-19485812#3",
	"to_edge_straight": "19457616#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19456179#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "19457616#4",
	"to_edge_right": "-19457616#3",
	"to_edge_straight": "-19485812#3"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "-19457616#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19485812#3",
	"to_edge_right": "-19456179#4",
	"to_edge_straight": "-19457616#3"
})


# Second Intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19456179#0",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19463160#7",
	"to_edge_right": "19463160#8",
	"to_edge_straight": "-19479801#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19463160#7",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19479801#4",
	"to_edge_right": "19456179#0",
	"to_edge_straight": "19463160#8"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19479801#4",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19463160#8",
	"to_edge_right": "-19463160#7",
	"to_edge_straight": "19456179#0"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19463160#8",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19456179#0",
	"to_edge_right": "-19479801#4",
	"to_edge_straight": "-19463160#7"
})


db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19485812#0",
	"to_edge_id": "19479801#3",    
    "vehicle_count": "100"
})  

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#2",
	"to_edge_id": "19457616#7",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#7", 
	"to_edge_id": "19457616#2",	
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479459#3",
	"to_edge_id": "19479457#1",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479457#1",
	"to_edge_id": "19479459#3",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479801#3",
	"to_edge_id": "19485812#0",
    "vehicle_count": "100"
})

''' This is just to see the 
schema of the table
db.trafficlightlogic.insert({
	"intersection_id": "202305472",
	"light_index": "0",
	"state": "Green",
	"duration": "30",
	"creation_time": datetime.now()
})
'''

######### create junctions and associate with groups

junction_id = db.junction.insert({
    "_id": "202305458",
    "west_lane_out":"19456179#0",
    "west_lane_out_values":["19456179#0_0","19456179#0_1"],
    "west_lane_in":"-19456179#0",
    "west_lane_in_values":["-19456179#0_1", "-19456179#0_0"],
    # left, straight, right
    "west_lane_in_adjascent":["-19463160#7", "-19479801#4", "19463160#8"],
    
    "east_lane_in":"19479801#4",
    "east_lane_in_values":["19479801#4_0","19479801#4_1"],
    "east_lane_in_adjascent":["19463160#8", "19456179#0", "-19463160#7"],
    "east_lane_out":"-19479801#4",
    "east_lane_out_values":["-19479801#4_1", "-19479801#4_0"],
    
    "north_lane_in":"19463160#7",
    "north_lane_in_values":["19463160#7_0","19463160#7_1"],
    "north_lane_in_adjascent":["-19479801#4", "19463160#8", "19456179#0"],
    "north_lane_out":"-19463160#7",
    "north_lane_out_values":["-19463160#7_1","-19463160#7_0"],
    
    "south_lane_out":"19463160#8",
    "south_lane_out_values":["19463160#8_0","19463160#8_1"],
    "south_lane_in":"-19463160#8",
    "south_lane_in_values":["-19463160#8_1", "-19463160#8_0"],
    "south_lane_in_adjascent":["19456179#0", "-19463160#7", "-19479801#4"]
})

db.inductionloop.insert({
    "_id": "19456179#0_0_5",
    "junction": junction_id,    
    "location": "west_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19456179#0_1_5",
    "junction": junction_id,    
    "location": "west_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19456179#0_0_-5",
    "junction": junction_id,    
    "location": "west_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19456179#0_1_-5",
    "junction": junction_id,    
    "location": "west_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19479801#4_0_5",
    "junction": junction_id,    
    "location": "east_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19479801#4_1_5",
    "junction": junction_id,    
    "location": "east_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19479801#4_0_-5",
    "junction": junction_id,    
    "location": "east_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19479801#4_1_-5",
    "junction": junction_id,    
    "location": "east_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19463160#7_0_5",
    "junction": junction_id,    
    "location": "north_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19463160#7_1_5",
    "junction": junction_id,    
    "location": "north_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19463160#7_0_-5",
    "junction": junction_id,    
    "location": "north_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19463160#7_1_-5",
    "junction": junction_id,    
    "location": "north_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "19463160#8_0_5",
    "junction": junction_id,    
    "location": "south_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "19463160#8_1_5",
    "junction": junction_id,    
    "location": "south_lane_out",
    "pos": 5
})

db.inductionloop.insert({
    "_id": "-19463160#8_0_-5",
    "junction": junction_id,    
    "location": "south_lane_in",
    "pos": -5
})

db.inductionloop.insert({
    "_id": "-19463160#8_1_-5",
    "junction": junction_id,    
    "location": "south_lane_in",
    "pos": -5
})   

simulation_id = db.simulation.insert({
    "group_id": "group_B",
    "duration": 1000,
    "junctions":[junction_id],
    "latitude": "36.137668",
    "longitude": "-86.800743",
    "update_rate": 2,
    "max_update_size": 50,
    "mode": "GROUP",
    "problem_id":"1",
    "status": "ACTIVE"
})
TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state, duration)

db.simulation_association.insert({
    "sim_id" : simulation_id,
    "sim_asso" : [simulation_id]
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Car",
	"accel": "20",
	"decel": "30",
	"sigma": "1",
	"max_speed": "100",
	"length": "10",
	"probability": "0.5"
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Bus",
	"accel": "15",
	"decel": "25",
	"sigma": "1",
	"max_speed": "70",
	"length": "15",
	"probability": "0.3"
})

db.vehicle.insert({
	"name": "Truck",
	"simulation_id": simulation_id, 
	"accel": "10",
	"decel": "15",
	"sigma": "1",
	"max_speed": "50",
	"length": "20",
	"probability": "0.2"
})

# First intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19485812#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19457616#3",
	"to_edge_right": "19457616#4",
	"to_edge_straight": "-19456179#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19457616#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19456179#4",
	"to_edge_right": "-19485812#3",
	"to_edge_straight": "19457616#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19456179#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "19457616#4",
	"to_edge_right": "-19457616#3",
	"to_edge_straight": "-19485812#3"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "-19457616#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19485812#3",
	"to_edge_right": "-19456179#4",
	"to_edge_straight": "-19457616#3"
})


# Second Intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19456179#0",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19463160#7",
	"to_edge_right": "19463160#8",
	"to_edge_straight": "-19479801#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19463160#7",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19479801#4",
	"to_edge_right": "19456179#0",
	"to_edge_straight": "19463160#8"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19479801#4",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19463160#8",
	"to_edge_right": "-19463160#7",
	"to_edge_straight": "19456179#0"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19463160#8",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19456179#0",
	"to_edge_right": "-19479801#4",
	"to_edge_straight": "-19463160#7"
})


db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19485812#0",
	"to_edge_id": "19479801#3",    
    "vehicle_count": "50"
})  

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#2",
	"to_edge_id": "19457616#7",
    "vehicle_count": "50"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#7", 
	"to_edge_id": "19457616#2",	
    "vehicle_count": "50"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479459#3",
	"to_edge_id": "19479457#1",
    "vehicle_count": "50"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479457#1",
	"to_edge_id": "19479459#3",
    "vehicle_count": "50"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479801#3",
	"to_edge_id": "19485812#0",
    "vehicle_count": "50"
})

simulation_id = db.simulation.insert({
    "group_id": "group_B",
    "duration": 1000,
    "junctions":[junction_id],
    "latitude": "36.137668",
    "longitude": "-86.800743",
    "update_rate": 2,
    "max_update_size": 50,
    "mode": "COLAB",
    "problem_id":"1",
    "status": "ACTIVE"
})
TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state, duration)

colab_id2 = simulation_id

db.simulation_association.insert({
    "sim_id" : simulation_id,
    "sim_asso" : [simulation_id]
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Car",
	"accel": "20",
	"decel": "30",
	"sigma": "1",
	"max_speed": "100",
	"length": "10",
	"probability": "0.5"
})

db.vehicle.insert({
	"simulation_id": simulation_id, 
	"name": "Bus",
	"accel": "15",
	"decel": "25",
	"sigma": "1",
	"max_speed": "70",
	"length": "15",
	"probability": "0.3"
})

db.vehicle.insert({
	"name": "Truck",
	"simulation_id": simulation_id, 
	"accel": "10",
	"decel": "15",
	"sigma": "1",
	"max_speed": "50",
	"length": "20",
	"probability": "0.2"
})

# First intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19485812#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19457616#3",
	"to_edge_right": "19457616#4",
	"to_edge_straight": "-19456179#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19457616#3",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19456179#4",
	"to_edge_right": "-19485812#3",
	"to_edge_straight": "19457616#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "19456179#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "19457616#4",
	"to_edge_right": "-19457616#3",
	"to_edge_straight": "-19485812#3"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305472", 
	"edge_id": "-19457616#4",
	"left_turn": "0.2",
	"right_turn": "0.2",
	"go_straight": "0.6",
	"to_edge_left": "-19485812#3",
	"to_edge_right": "-19456179#4",
	"to_edge_straight": "-19457616#3"
})


# Second Intersection
db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19456179#0",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19463160#7",
	"to_edge_right": "19463160#8",
	"to_edge_straight": "-19479801#4"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19463160#7",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "-19479801#4",
	"to_edge_right": "19456179#0",
	"to_edge_straight": "19463160#8"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "19479801#4",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19463160#8",
	"to_edge_right": "-19463160#7",
	"to_edge_straight": "19456179#0"
})

db.turnprobability.insert({
	"simulation_id": simulation_id, 
	"intersection_id": "202305458", 
	"edge_id": "-19463160#8",
	"left_turn": "0.3",
	"right_turn": "0.3",
	"go_straight": "0.4",
	"to_edge_left": "19456179#0",
	"to_edge_right": "-19479801#4",
	"to_edge_straight": "-19463160#7"
})

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19485812#0",
	"to_edge_id": "19479801#3",    
    "vehicle_count": "100"
})  

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#2",
	"to_edge_id": "19457616#7",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19457616#7", 
	"to_edge_id": "19457616#2",	
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479459#3",
	"to_edge_id": "19479457#1",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479457#1",
	"to_edge_id": "19479459#3",
    "vehicle_count": "100"
}) 

db.flows.insert({
    "simulation_id": simulation_id,
    "from_edge_id": "19479801#3",
	"to_edge_id": "19485812#0",
    "vehicle_count": "100"
})

db.simulation_association.insert({
    "sim_id" : colab_id,
    "sim_asso" : [colab_id1, colab_id2],
    "is_master" : True
})

db.problems.insert({
    "problem_id": "1",
    "title": "Analyse the effects of each Vehicle parameters on average wait time?",
    "description":"How does vehicle acceleration, deceleration, max speed, length effect the avg. wait time, and fuel consumed? (What do you suggest to vehicle manufacturers after interpreting the results?)",
    "solution": "",
})

db.problems.insert({
    "problem_id": "2",
    "title": "Analyse the effects of each Vehicle parameter and driver's imperfection on average wait time?",
    "description":"How does the driver imperfection impacts the avg. wait time and avg. travel time? (What do you suggest to drivers? Aggressive driving or maintaining constant velocity? Driver imperfection refers to the inability of a driver to maintain constant velocity, causing fluctuations in speed that affect the vehicles behind)",
    "solution": "",
})

db.problems.insert({
    "problem_id": "3",
    "title": "Analyse the effects of Turning Probabilities on average wait time?",
    "description":"How does closing one lane (which will reroute vehicles over it) affect the avg. wait time? (Close one lane by setting turning probability to zero towards that lane.) What do you suggest to TDOT on closing lanes?",
    "solution": "",
})

db.problems.insert({
    "problem_id": "4",
    "title": "How to reduce avg. wait time by programming traffic light on your intersection?",
    "description":"How could you reduce wait time below 1.8 sec by default vehicle and turning probability parameters? (Reset parameters to factory settings) What do you suggest to traffic engineers on programming traffic lights?",
    "solution": "",
})

# Schema for reason for changing parameters is as follows
# No need to create this table. Automatically created.
#db.reason_change({
#	"simulation_id": "
#	"user_id": ""
#	"description":"",
#})

