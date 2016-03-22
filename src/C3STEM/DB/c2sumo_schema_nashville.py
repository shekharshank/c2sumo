import pymongo
from pymongo import MongoClient
from datetime import datetime
import sys
import bcrypt
sys.path.insert(0, '/opt/C3STEM/Middleware')
from DAO import TrafficLightDAO

from simulation_related import *
from junction1_data_nashville import *
from junction2_data_nashville import *
from default_problem_data import *

connection = MongoClient()
connection.drop_database('c3stem_database')
db = connection.c3stem_database

intersection_id1 = "202305472"
intersection_id2 = "202305458"


############ create junctions and induction loops ####
#### intersection_id1 - "202305472" ###
createjunction1DefaultData(intersection_id1)

#### intersection_id2 - "202305458" ###
createjunction2DefaultData(intersection_id2)

########## end junctions ###########


######################### problems #########################


db.problems.insert({
    "problem_id": "3",
    "title": "Analyze the effects of vehicle parameters for a 4-way stop sign:",
    "description":"How do vehicle acceleration, deceleration, max speed, and length affect the average waiting time and average speed of the vehicles?  How are vehicle throughput (flow rate), queue length and queue duration at the assigned intersection affected? Report the impact of vehicle parameters.",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_all_way",
    "type": "STOP_SIGN"
})

db.problems.insert({
    "problem_id": "6a",
    "title": "Analyze the effects of traffic flows, vehicle types, turning probabilities for unprotected left turns:",
    "description":"Compare the three mechanisms based on the average waiting time, average speed, vehicle throughput (flow rate), and queue length and queue duration at the intersection. Based on these, can you suggest in what scenarios you prefer one over the other? Some mechanisms provide better performance (in terms of waiting time etc.) however, what could be drawbacks of those mechanisms? Change the vehicle flow rate and make vehicle type homogeneous by making other vehicle probability as zero. How does this impact the results?",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_priority",
    "type": "UNPROTECTED"
})

db.problems.insert({
    "problem_id": "6b",
    "title": "Analyze the effects of traffic flows, vehicle types, turning probabilities for left turns with stop sign:",
    "description":"Compare the three mechanisms based on the average waiting time, average speed, vehicle throughput (flow rate), and queue length and queue duration at the intersection. Based on these, can you suggest in what scenarios you prefer one over the other? Some mechanisms provide better performance (in terms of waiting time etc.) however, what could be drawbacks of those mechanisms? Change the vehicle flow rate and make vehicle type homogeneous by making other vehicle probability as zero. How does this impact the results?",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_all_way",
    "type": "STOP_SIGN"
})

db.problems.insert({
    "problem_id": "6c",
    "title": "Analyze the effects of traffic flows, vehicle types, turning probabilities for left turns with traffic lights:",
    "description":"Compare the three mechanisms based on the average waiting time, average speed, vehicle throughput (flow rate), and queue length and queue duration at the intersection. Based on these, can you suggest in what scenarios you prefer one over the other? Some mechanisms provide better performance (in terms of waiting time etc.) however, what could be drawbacks of those mechanisms? Change the vehicle flow rate and make vehicle type homogeneous by making other vehicle probability as zero. How does this impact the results?",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_traffic",
    "type": "TRAFFIC_SIGNAL"
})

db.problems.insert({
    "problem_id": "8",
    "title": "Model the traffic lights at a single intersection that you have been put in charge of",
    "description":"Try out values for your traffic light timing sequences, with the goal of finding the right timing sequence that maximizes the flow of traffic through your intersection. Remember the flow is measured by the throughput parameter that was discussed in the last unit.",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_traffic",
    "type": "TRAFFIC_SIGNAL"
})

db.problems.insert({
    "problem_id": "10a",
    "title": "Optimize traffic light phases to improve traffic through your intersection:",
    "description":"By experimenting with the timing of the traffic light phases, try to produce the minimum average wait time at both intersections (with the default vehicle parameters, turning probabilities, and traffic input flow rates)? Based on these results, how would you describe a general approach that traffic engineers could follow for programming traffic lights?",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_traffic",
    "type": "TRAFFIC_SIGNAL"
})

db.problems.insert({
    "problem_id": "10b",
    "title": "Analyze the effects of driver behavior:",
    "description":"A basic model of driver behavior in traffic simulations uses a parameter called driver imperfection, which describes the extent to which drivers allow their speed to fluctuate instead of maintaining a constant speed. These fluctuations affect the vehicles behind a driver, which have to react to the driver's changing speed. By experimenting with different values for the driver imperfection parameter in this simulation, describe and generate plots of how this aspect of driver behavior impacts the average wait time.",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_traffic",
    "type": "TRAFFIC_SIGNAL"
})

db.problems.insert({
    "problem_id": "10c",
    "title": "Analyze the effects of lane closures:",
    "description":"How does closing one lane (which will reroute vehicles to other lanes) affect the average wait time? You can simulate a lane closure by adjusting the turning probabilities at an intersection so the vehicles will never enter the lane. Experiment to identify changes that can decrease the average wait time with the lane closure.",
    "solution": "",
    "map_loc_prefix": "/opt/C3STEM/Map/westend_traffic",
    "type": "TRAFFIC_SIGNAL"
})

########## end problems ###########

################## create default values for each problem ###############

all_problems = db.problems.find()
for problem in all_problems:
	sim_id1 = "DEFAULT_" + problem.get('problem_id') + '_' + intersection_id1
	sim_id2 = "DEFAULT_" + problem.get('problem_id') + '_' + intersection_id2
	sim_id_no_junc = "DEFAULT_" + problem.get('problem_id')
	if(problem.get('problem_id') == "6c"):
		createJunction1_6c_TrafficData(intersection_id1, sim_id1)
		createJunction2_6c_TrafficData(intersection_id2, sim_id2)
	else:
		createJunction1DefaultTrafficData(intersection_id1, sim_id1)
		createJunction2DefaultTrafficData(intersection_id2, sim_id2)
		
	createJunction1DefaultTurnProbability(intersection_id1, sim_id1)	
	createJunction1DefaultFlowData(intersection_id1, sim_id1)
	createJunction2DefaultTurnProbability(intersection_id2, sim_id2)	
	createJunction2DefaultFlowData(intersection_id2, sim_id2)
	createDefaultVehicleData(sim_id_no_junc)
	

############ groups ##############
hashed_pass = bcrypt.hashpw("pass1234".encode('utf-8'), bcrypt.gensalt())

for j in range (0, 10):
	default_prob = "3"
	grp_number_type1 = str(j + 1)
	master_grp_id = "group_" + grp_number_type1
	lat1 = "36.138724"
	long1 = "-86.810884"
	collaboration_url_admin = "https://plus.google.com/hangouts/_?gid=000000000000"
	
	group_id = db.studentgroup.insert({
	    "_id": master_grp_id,
	    "name": grp_number_type1,
	    "group_type": "A",
	    "latitude": lat1, 
	    "longitude": long1,
		"collaboration_url": "",
		"collaboration_url_admin": collaboration_url_admin,
		"last_update_time": str(datetime.now())
	})
	
	sim_id = createGroupSimData(group_id, master_grp_id, lat1, long1, "GROUP", default_prob, [intersection_id1])
	createTurnProbability(sim_id, default_prob, intersection_id1)
	createFlowData(sim_id, default_prob, intersection_id1)
	createVehicleData(sim_id, default_prob)
	createTrafficLightData(sim_id, default_prob, intersection_id1)
	
	colab_default_prob = "10a"
	
	colab_group_id1 = db.studentgroup.insert({
	    "_id": master_grp_id + "_A_COLAB",
	    "name": grp_number_type1 + " A",
	    "group_type": "A",
	    "latitude": lat1, 
	    "longitude": long1,
		"collaboration_url": "",
		"collaboration_url_admin": collaboration_url_admin,
		"last_update_time": str(datetime.now())
	})
	
	colab_sim_id1 = createGroupSimData(colab_group_id1, master_grp_id, lat1, long1, "COLAB", colab_default_prob, [intersection_id1])
	createTurnProbability(colab_sim_id1, colab_default_prob, intersection_id1)
	createFlowData(colab_sim_id1, colab_default_prob, intersection_id1)
	createVehicleData(colab_sim_id1, colab_default_prob)
	createTrafficLightData(colab_sim_id1, colab_default_prob, intersection_id1)
	
	lat2 = "36.137668"
	long2 = "-86.800743"
	
	colab_group_id2 = db.studentgroup.insert({
		    "_id": master_grp_id + "_B_COLAB",
		    "name": grp_number_type1 + " B",
		    "group_type": "B",
		    "latitude": lat2, 
		    "longitude": long2,
			"collaboration_url": "",
			"collaboration_url_admin": collaboration_url_admin,
			"last_update_time": str(datetime.now())
	})
	
	colab_sim_id2 = createGroupSimData(colab_group_id2, master_grp_id, lat2, long2, "COLAB", colab_default_prob, [intersection_id2])
	createTurnProbability(colab_sim_id2, colab_default_prob, intersection_id2)
	createFlowData(colab_sim_id2, colab_default_prob, intersection_id2)
	createVehicleData(colab_sim_id2, colab_default_prob)
	createTrafficLightData(colab_sim_id2, colab_default_prob, intersection_id2)

	
	lat = "36.138099"
	long = "-86.806004"
	
	colab_group_id = db.studentgroup.insert({
		    "_id": master_grp_id + "_C_COLAB",
		    "name": grp_number_type1 + " Admin",
		    "group_type": "C",
		    "latitude": lat, 
		    "longitude": long,
			"collaboration_url": "",
			"collaboration_url_admin": collaboration_url_admin,
			"last_update_time": str(datetime.now())
	})
	
	colab_sim_id = createGroupSimData(colab_group_id, master_grp_id, lat, long, "COLAB", colab_default_prob, [intersection_id1, intersection_id2],
				[colab_sim_id1, colab_sim_id2])	
				
	colab_user_id = db.student.insert({
		    "_id": "admin_colab_" + grp_number_type1,
		    "firstname": "Admin",
		    "lastname": grp_number_type1,
		    "group_id": colab_group_id,
		    "master_group_id": master_grp_id,
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

for i in range (1, 31):
	seq = 100 + i;
	user_id = "user" + str(seq);
	db.student.insert({
	    "_id": user_id,
	    "firstname": "User",
	    "lastname": str(seq),
	    "schoolname": "School X",
	    "schoolcode": "schoolx",
	    "group_id": "group_" + str(((i-1)/2)+1),
	    "password": hashed_pass,
	    "changepassword": "true"
	})

	db.user_studentrole.insert({
	    "user_id": user_id,
	    "role_ids": ["user","admin"],
	    "user_mode": "INDIVIDUAL"
	})
	
	
db.student.insert({
	    "_id": 'sneezy',
	    "firstname": "User",
	    "lastname": 'smith',
	    "schoolname": "School X",
	    "schoolcode": "schoolx",
	    "group_id": "group_11",
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
	    "group_id": "group_12",
	    "password": hashed_pass,
	    "changepassword": "true"
	})

db.user_studentrole.insert({
	    "user_id": 'sleepy',
	    "role_ids": ["user","admin"]
	})


db.student.update ({'_id' : 'user101'}, {"$set": {'group_id' : 'group_1', 'master_group_id' : 'group_1', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user102'}, {"$set": {'group_id' : 'group_1', 'master_group_id' : 'group_1', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user103'}, {"$set": {'group_id' : 'group_1', 'master_group_id' : 'group_1', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user103'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user104'}, {"$set": {'group_id' : 'group_2', 'master_group_id' : 'group_2', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user105'}, {"$set": {'group_id' : 'group_2', 'master_group_id' : 'group_2', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user106'}, {"$set": {'group_id' : 'group_2', 'master_group_id' : 'group_2', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user106'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user107'}, {"$set": {'group_id' : 'group_3', 'master_group_id' : 'group_3', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user108'}, {"$set": {'group_id' : 'group_3', 'master_group_id' : 'group_3', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user109'}, {"$set": {'group_id' : 'group_3', 'master_group_id' : 'group_3', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user109'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user110'}, {"$set": {'group_id' : 'group_4', 'master_group_id' : 'group_4', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user111'}, {"$set": {'group_id' : 'group_4', 'master_group_id' : 'group_4', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user112'}, {"$set": {'group_id' : 'group_4', 'master_group_id' : 'group_4', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user112'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user113'}, {"$set": {'group_id' : 'group_5', 'master_group_id' : 'group_5', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user114'}, {"$set": {'group_id' : 'group_5', 'master_group_id' : 'group_5', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user115'}, {"$set": {'group_id' : 'group_5', 'master_group_id' : 'group_5','colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user115'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user116'}, {"$set": {'group_id' : 'group_6', 'master_group_id' : 'group_6', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user117'}, {"$set": {'group_id' : 'group_6', 'master_group_id' : 'group_6', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user118'}, {"$set": {'group_id' : 'group_6', 'master_group_id' : 'group_6', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user118'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user119'}, {"$set": {'group_id' : 'group_7', 'master_group_id' : 'group_7', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user120'}, {"$set": {'group_id' : 'group_7', 'master_group_id' : 'group_7', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user121'}, {"$set": {'group_id' : 'group_7', 'master_group_id' : 'group_7', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user121'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user122'}, {"$set": {'group_id' : 'group_8', 'master_group_id' : 'group_8', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user123'}, {"$set": {'group_id' : 'group_8', 'master_group_id' : 'group_8', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user124'}, {"$set": {'group_id' : 'group_8', 'master_group_id' : 'group_8', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user124'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user125'}, {"$set": {'group_id' : 'group_9', 'master_group_id' : 'group_9', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user126'}, {"$set": {'group_id' : 'group_9', 'master_group_id' : 'group_9', 'colab_group_type' : 'B'}})

db.student.update ({'_id' : 'user127'}, {"$set": {'group_id' : 'group_9', 'master_group_id' : 'group_9', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user127'}, {"$set": {'role_ids' : ['user','admin','super']}})
db.student.update ({'_id' : 'user128'}, {"$set": {'group_id' : 'group_10', 'master_group_id' : 'group_10', 'colab_group_type' : 'A'}})
db.student.update ({'_id' : 'user129'}, {"$set": {'group_id' : 'group_10', 'master_group_id' : 'group_10', 'colab_group_type' : 'B'}})
db.student.update ({'_id' : 'user130'}, {"$set": {'group_id' : 'group_10', 'master_group_id' : 'group_10', 'colab_group_type' : 'C'}})
db.user_studentrole.update({'user_id': 'user130'}, {"$set": {'role_ids' : ['user','admin','super']}})


# Schema for reason for changing parameters is as follows
# No need to create this table. Automatically created.
#db.reason_change({
#	"simulation_id": "
#	"user_id": ""
#	"description":"",
#})

print 'Done!'

