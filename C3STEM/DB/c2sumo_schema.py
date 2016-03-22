import pymongo
from pymongo import MongoClient
from datetime import datetime
import sys
import bcrypt
sys.path.insert(0, '/opt/C3STEM/Middleware')
from DAO import TrafficLightDAO

from simulation_related import *
from junction1_data import *
from junction2_data import *
from junction3_data import *
from junction4_data import *

connection = MongoClient()
db = connection.c3stem_database

intersection_id1 = "202601366"
intersection_id2 = "202662093"
intersection_id3 = "202666904"
intersection_id4 = "202666877"


############ create junctions and induction loops ####
#### intersection_id1 - "202601366" ###
createjunction1Data(intersection_id1)

#### intersection_id2 - "202662093" ###
createjunction2Data(intersection_id2)

#### intersection_id3 - "202666904" ###
createjunction3Data(intersection_id3)

#### intersection_id3 - "202666877" ###
createjunction4Data(intersection_id4)

########## end junctions ###########


############ groups ##############
hashed_pass = bcrypt.hashpw("pass1234".encode('utf-8'), bcrypt.gensalt())

for j in range (0, 5):
	colab_group_id = "group_" + str(j*4 + 1) + "_" + str(j*4 + 2) + "_" + str(j*4 + 3) + "_" + str(j*4 + 4)
	group_id = db.studentgroup.insert({
	    "_id": "group_" + str(j*4 + 1),
	    "name": str(j*4 + 1),
	    "group_type": "A",
	    "latitude": "35.038943",
	    "longitude": "-85.282864",
		"collaboration_url": "",
		"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
		"last_update_time": str(datetime.now()),
	    "colab_group_id" : colab_group_id
	})
	
	sim_id1 = createGroupSimData(group_id, "35.038943", "-85.282864", "GROUP", [intersection_id1])
	colab_sim_id1 = createGroupSimData(group_id, "35.038943", "-85.282864", "COLAB", [intersection_id1])
	
	createJunction1TurnProbability(intersection_id1, sim_id1)
	createJunction1TurnProbability(intersection_id1, colab_sim_id1)
	
	createJunction1FlowData(intersection_id1, sim_id1)
	createJunction1FlowData(intersection_id1, colab_sim_id1)
	createJunction2FlowData(intersection_id2, colab_sim_id1)
	createJunction3FlowData(intersection_id3, colab_sim_id1)
	createJunction4FlowData(intersection_id4, colab_sim_id1)

	group_id = db.studentgroup.insert({
	    "_id": "group_" + str(j*4 + 2),
	    "name": str(j*4 + 2),
	    "group_type": "B",
	    "latitude": "35.034852",
	    "longitude": "-85.271851",
		"collaboration_url": "",
		"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
		"last_update_time": str(datetime.now()),
	    "colab_group_id" : colab_group_id
	})
	
	sim_id2 = createGroupSimData(group_id, "35.034852", "-85.271851", "GROUP", [intersection_id2])
	colab_sim_id2 = createGroupSimData(group_id, "35.034852", "-85.271851", "COLAB", [intersection_id2])
	
	createJunction2TurnProbability(intersection_id2, sim_id2)
	createJunction2TurnProbability(intersection_id2, colab_sim_id2)
	
	createJunction2FlowData(intersection_id2, sim_id2)
	createJunction1FlowData(intersection_id1, colab_sim_id2)
	createJunction2FlowData(intersection_id2, colab_sim_id2)
	createJunction3FlowData(intersection_id3, colab_sim_id2)
	createJunction4FlowData(intersection_id4, colab_sim_id2)

	group_id = db.studentgroup.insert({
	    "_id": "group_" + str(j*4 + 3),
	    "name": str(j*4 + 3),
	    "group_type": "C",
	    "latitude": "35.032382",
	    "longitude": "-85.273227",
		"collaboration_url": "",
		"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
		"last_update_time": str(datetime.now()),
	    "colab_group_id" : colab_group_id
	})
	
	sim_id3 = createGroupSimData(group_id, "35.032382", "-85.273227", "GROUP", [intersection_id3])
	colab_sim_id3 = createGroupSimData(group_id, "35.032382", "-85.273227", "COLAB", [intersection_id3])
	
	createJunction3TurnProbability(intersection_id3, sim_id3)
	createJunction3TurnProbability(intersection_id3, colab_sim_id3)
	
	createJunction3FlowData(intersection_id3, sim_id3)
	createJunction1FlowData(intersection_id1, colab_sim_id3)
	createJunction2FlowData(intersection_id2, colab_sim_id3)
	createJunction3FlowData(intersection_id3, colab_sim_id3)
	createJunction4FlowData(intersection_id4, colab_sim_id3)
	
	group_id = db.studentgroup.insert({
	    "_id": "group_" + str(j*4 + 4),
	    "name": str(j*4 + 4),
	    "group_type": "D",
	    "latitude": "35.036482",
	    "longitude": "-85.284245",
		"collaboration_url": "",
		"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000",
		"last_update_time": str(datetime.now()),
	    "colab_group_id" : colab_group_id
	})
	
	sim_id4 = createGroupSimData(group_id, "35.036482", "-85.284245", "GROUP", [intersection_id4])
	colab_sim_id4 = createGroupSimData(group_id, "35.036482", "-85.284245", "COLAB", [intersection_id4])
		
	createJunction4TurnProbability(intersection_id4, sim_id4)
	createJunction4TurnProbability(intersection_id4, colab_sim_id4)
		
	createJunction4FlowData(intersection_id4, sim_id4)
	createJunction1FlowData(intersection_id1, colab_sim_id4)
	createJunction2FlowData(intersection_id2, colab_sim_id4)
	createJunction3FlowData(intersection_id3, colab_sim_id4)
	createJunction4FlowData(intersection_id4, colab_sim_id4)
	
	# colab groups
	group_id = db.studentgroup.insert({
	    "_id": colab_group_id,
	    "name": str(j*4 + 1) + " " + str(j*4 + 2) + " " + str(j*4 + 3) + " " + str(j*4 + 4) + " colab",
	    "group_type": "COLAB",
	    "latitude": "35.035652",
	    "longitude": "-85.278242",
		"collaboration_url": "",
		"collaboration_url_admin": "https://plus.google.com/hangouts/_?gid=000000000000"    ,
		"last_update_time": str(datetime.now()),
	    "colab_group_id" : colab_group_id
	})

	#colab simulation
	colab_sim_id = createGroupSimData(group_id, "35.035652", "-85.278242", "COLAB",
			[intersection_id1, intersection_id2, intersection_id3, intersection_id4],
				[colab_sim_id1, colab_sim_id2, colab_sim_id3, colab_sim_id4])
	
	
	colab_user_id = db.student.insert({
	    "_id": "admin_colab_" + str(j+1),
	    "firstname": "Admin",
	    "lastname": str(j+1),
	    "group_id": colab_group_id,
	    "password": hashed_pass
	})
	
	db.user_studentrole.insert({
	    "user_id": colab_user_id,
	    "role_ids": ["user", "admin", "super"],
	    "user_mode": "COLAB"
	})


##################### user and their roles


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

for i in range (1, 81):
	user_id = "user" + str(i);
	db.student.insert({
	    "_id": user_id,
	    "firstname": "User",
	    "lastname": str(i),
	    "schoolname": "School X",
	    "schoolcode": "schoolx",
	    "group_id": "group_" + str(((i-1)/4)+1),
	    "password": hashed_pass,
	    "changepassword": "true"
	})

	db.user_studentrole.insert({
	    "user_id": user_id,
	    "role_ids": ["user","admin"]
	})
	
	
db.student.insert({
	    "_id": 'sneezy',
	    "firstname": "User",
	    "lastname": 'smith',
	    "schoolname": "School X",
	    "schoolcode": "schoolx",
	    "group_id": "group_17",
	    "password": hashed_pass,
	    "changepassword": "true"
	})

db.user_studentrole.insert({
	    "user_id": 'sneezy',
	    "role_ids": ["user","admin"]
	})

db.student.insert({
	    "_id": 'sleepy',
	    "firstname": "User",
	    "lastname": 'smith',
	    "schoolname": "School X",
	    "schoolcode": "schoolx",
	    "group_id": "group_18",
	    "password": hashed_pass,
	    "changepassword": "true"
	})

db.user_studentrole.insert({
	    "user_id": 'sleepy',
	    "role_ids": ["user","admin"]
	})


db.student.update ({'_id' : 'user1'}, {"$set": {'group_id' : 'group_1'}})
db.student.update ({'_id' : 'user2'}, {"$set": {'group_id' : 'group_1'}})
db.student.update ({'_id' : 'user11'}, {"$set": {'group_id' : 'group_1'}})
db.student.update ({'_id' : 'user12'}, {"$set": {'group_id' : 'group_1'}})
db.student.update ({'_id' : 'user3'}, {"$set": {'group_id' : 'group_2'}})
db.student.update ({'_id' : 'user4'}, {"$set": {'group_id' : 'group_2'}})
db.student.update ({'_id' : 'user5'}, {"$set": {'group_id' : 'group_2'}})
db.student.update ({'_id' : 'user6'}, {"$set": {'group_id' : 'group_2'}})
db.student.update ({'_id' : 'user7'}, {"$set": {'group_id' : 'group_3'}})
db.student.update ({'_id' : 'user8'}, {"$set": {'group_id' : 'group_3'}})
db.student.update ({'_id' : 'user9'}, {"$set": {'group_id' : 'group_3'}})
db.student.update ({'_id' : 'user10'}, {"$set": {'group_id' : 'group_3'}})
db.student.update ({'_id' : 'user13'}, {"$set": {'group_id' : 'group_4'}})
db.student.update ({'_id' : 'user14'}, {"$set": {'group_id' : 'group_4'}})
db.student.update ({'_id' : 'user15'}, {"$set": {'group_id' : 'group_4'}})
db.student.update ({'_id' : 'user16'}, {"$set": {'group_id' : 'group_4'}})
db.student.update ({'_id' : 'user17'}, {"$set": {'group_id' : 'group_5'}})
db.student.update ({'_id' : 'user18'}, {"$set": {'group_id' : 'group_5'}})
db.student.update ({'_id' : 'user19'}, {"$set": {'group_id' : 'group_5'}})
db.student.update ({'_id' : 'user20'}, {"$set": {'group_id' : 'group_5'}})
db.student.update ({'_id' : 'user21'}, {"$set": {'group_id' : 'group_6'}})
db.student.update ({'_id' : 'user22'}, {"$set": {'group_id' : 'group_6'}})
db.student.update ({'_id' : 'user23'}, {"$set": {'group_id' : 'group_6'}})
db.student.update ({'_id' : 'user24'}, {"$set": {'group_id' : 'group_6'}})
db.student.update ({'_id' : 'user37'}, {"$set": {'group_id' : 'group_7'}})
db.student.update ({'_id' : 'user38'}, {"$set": {'group_id' : 'group_7'}})
db.student.update ({'_id' : 'user39'}, {"$set": {'group_id' : 'group_7'}})
db.student.update ({'_id' : 'user40'}, {"$set": {'group_id' : 'group_7'}})
db.student.update ({'_id' : 'user33'}, {"$set": {'group_id' : 'group_8'}})
db.student.update ({'_id' : 'user34'}, {"$set": {'group_id' : 'group_8'}})
db.student.update ({'_id' : 'user35'}, {"$set": {'group_id' : 'group_8'}})
db.student.update ({'_id' : 'user36'}, {"$set": {'group_id' : 'group_8'}})
db.student.update ({'_id' : 'user29'}, {"$set": {'group_id' : 'group_9'}})
db.student.update ({'_id' : 'user30'}, {"$set": {'group_id' : 'group_9'}})
db.student.update ({'_id' : 'user31'}, {"$set": {'group_id' : 'group_9'}})
db.student.update ({'_id' : 'user32'}, {"$set": {'group_id' : 'group_9'}})
db.student.update ({'_id' : 'user25'}, {"$set": {'group_id' : 'group_10'}})
db.student.update ({'_id' : 'user26'}, {"$set": {'group_id' : 'group_10'}})
db.student.update ({'_id' : 'user27'}, {"$set": {'group_id' : 'group_10'}})
db.student.update ({'_id' : 'user28'}, {"$set": {'group_id' : 'group_10'}})
db.student.update ({'_id' : 'user41'}, {"$set": {'group_id' : 'group_11'}})
db.student.update ({'_id' : 'user42'}, {"$set": {'group_id' : 'group_11'}})
db.student.update ({'_id' : 'user43'}, {"$set": {'group_id' : 'group_11'}})
db.student.update ({'_id' : 'user44'}, {"$set": {'group_id' : 'group_11'}})
db.student.update ({'_id' : 'user45'}, {"$set": {'group_id' : 'group_12'}})
db.student.update ({'_id' : 'user46'}, {"$set": {'group_id' : 'group_12'}})
db.student.update ({'_id' : 'user47'}, {"$set": {'group_id' : 'group_12'}})
db.student.update ({'_id' : 'user48'}, {"$set": {'group_id' : 'group_12'}})
db.student.update ({'_id' : 'user49'}, {"$set": {'group_id' : 'group_13'}})
db.student.update ({'_id' : 'user50'}, {"$set": {'group_id' : 'group_13'}})
db.student.update ({'_id' : 'user51'}, {"$set": {'group_id' : 'group_13'}})
db.student.update ({'_id' : 'user52'}, {"$set": {'group_id' : 'group_13'}})
db.student.update ({'_id' : 'user57'}, {"$set": {'group_id' : 'group_14'}})
db.student.update ({'_id' : 'user58'}, {"$set": {'group_id' : 'group_14'}})
db.student.update ({'_id' : 'user59'}, {"$set": {'group_id' : 'group_14'}})
db.student.update ({'_id' : 'user60'}, {"$set": {'group_id' : 'group_14'}})
db.student.update ({'_id' : 'user53'}, {"$set": {'group_id' : 'group_15'}})
db.student.update ({'_id' : 'user54'}, {"$set": {'group_id' : 'group_15'}})
db.student.update ({'_id' : 'user55'}, {"$set": {'group_id' : 'group_15'}})
db.student.update ({'_id' : 'user56'}, {"$set": {'group_id' : 'group_15'}})
db.student.update ({'_id' : 'user72'}, {"$set": {'group_id' : 'group_16'}})
db.student.update ({'_id' : 'user73'}, {"$set": {'group_id' : 'group_16'}})
db.student.update ({'_id' : 'user74'}, {"$set": {'group_id' : 'group_16'}})
db.student.update ({'_id' : 'user75'}, {"$set": {'group_id' : 'group_16'}})

######################### problems #########################

db.problems.insert({
    "problem_id": "1",
    "title": "Analyze the effects of vehicle parameters:",
    "description":"How do vehicle acceleration, deceleration, max speed, and length affect the average wait time? Would you guess how the quantity of fuel consumed is related to these parameters? What suggestions might you make to vehicle manufacturers based on these results?",
    "solution": "",
})

db.problems.insert({
    "problem_id": "2",
    "title": "Optimize traffic light cycles to improve traffic through your intersection:",
    "description":"By experimenting with the timing of the traffic light cycles, try to produce the minimum average wait time at your intersection (with the default vehicle parameters, turning probabilities, and traffic input flow rates)? Based on these results, how would you describe a general approach that traffic engineers could follow for programming traffic lights?",
    "solution": "",
})

db.problems.insert({
    "problem_id": "3",
    "title": "Analyze the effects of driver behavior:",
    "description":"A basic model of driver behavior in traffic simulations uses a parameter called driver imperfection, which describes the extent to which drivers allow their speed to fluctuate instead of maintaining a constant speed. These fluctuations affect the vehicles behind a driver, which have to react to the driver's changing speed.  By experimenting with different values for the driver imperfection parameter in this simulation, describe and generate plots of how this aspect of driver behavior impacts the average wait time.",
    "solution": "",
})

db.problems.insert({
    "problem_id": "4",
    "title": "Analyze the effects of lane closures:",
    "description":"How does closing one lane (which will reroute vehicles to other lanes) affect the average wait time? You can simulate a lane closure by adjusting the turning probabilities at an intersection so the vehicles will never enter the lane.  Start with the traffic light cycles you found in problem 3 and experiment to identify changes that can decrease the average wait time with the lane closure.",
    "solution": "",
})

# Schema for reason for changing parameters is as follows
# No need to create this table. Automatically created.
#db.reason_change({
#	"simulation_id": "
#	"user_id": ""
#	"description":"",
#})

