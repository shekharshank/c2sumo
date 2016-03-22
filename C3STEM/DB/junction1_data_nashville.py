from DBUtil import *
import sys
sys.path.insert(0, '/opt/C3STEM/Middleware')
from DAO import TrafficLightDAO
		
##### junction - 202305472
def createjunction1DefaultData(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,
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
	
def createJunction1DefaultTurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# First intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
		"edge_id": "-19457616#4",
		"left_turn": "0.2",
		"right_turn": "0.2",
		"go_straight": "0.6",
		"to_edge_left": "-19485812#3",
		"to_edge_right": "-19456179#4",
		"to_edge_straight": "-19457616#3"
	})
	
def createJunction1DefaultFlowData(intersection_id1, simulation_id):
	
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
	
	connection = MongoClient()
	junction_id = intersection_id1
	db = connection.c3stem_database
	db.flows.insert({
	    "point_name": "A",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19485812#3",
		"to_edge_id": "n/a",
		"via_edge_id": "19485812#3",
	    "flow_rate": "600",
		"latitude":"36.13950836759643",
		"longitude": "-86.8132084608078",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "B",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19457616#3",
		"to_edge_id": "n/a",
		"via_edge_id": "19457616#3",
	    "flow_rate": "600",
		"latitude": "36.13948079754408",
		"longitude": "-86.81078642606735",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Iwest",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19456179#4",
		"to_edge_id": "n/a",
		"via_edge_id": "19456179#4",
	    "flow_rate": "600",
		"latitude": "36.138289149356346",
		"longitude": "-86.80801838636398",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Ieast",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19456179#0",
		"to_edge_id": "n/a",
		"via_edge_id": "-19456179#0",
	    "flow_rate": "600",
		"latitude": "36.138387177653954",
		"longitude": "-86.80800765752792",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "Lnorth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19457616#4",
		"to_edge_id": "n/a",
		"via_edge_id": "-19457616#4",
	    "flow_rate": "600",
		"latitude": "36.13720010799972",
		"longitude": "-86.81105330586433",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Lsouth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19457616#4",
		"to_edge_id": "n/a",
		"via_edge_id": "-19457616#4",
	    "flow_rate": "600",
		"latitude": "36.137777562190905",
		"longitude": "-86.81101977825165",
		"removable": "1"
	})
	
	
def createJunction1DefaultTrafficData(junction_id, simulation_id):
		
	
	state0 = "Green!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Green!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state1 = "Yellow!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Yellow!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state2 = "Red!0g!1!2-Green!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Green!12!13-Red!14g!15-Red!16!17"
	state3 = "Red!0g!1!2-Yellow!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Yellow!12!13-Red!14g!15-Red!16!17"
	state4 = "Red!0g!1!2-Red!3!4-Green!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Green!14g!15-Red!16!17"
	state5 = "Red!0g!1!2-Red!3!4-Yellow!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Yellow!14g!15-Red!16!17"
	state6 = "Red!0g!1!2-Red!3!4-Red!5g!6-Green!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Green!16!17"
	state7 = "Red!0g!1!2-Red!3!4-Red!5g!6-Yellow!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Yellow!16!17"
	'''
	if junction_id == "202601366":
		state0 = "Green!0g!1-Red!2!3-Red!4g!5!6-Red!7!8-Green!9g!10-Red!11!12-Red!13g!14!15-Red!16!17"
		state1 = "Yellow!0g!1-Red!2!3-Red!4g!5!6-Red!7!8-Yellow!9g!10-Red!11!12-Red!13g!14!15-Red!16!17"
		state2 = "Red!0g!1-Green!2!3-Red!4g!5!6-Red!7!8-Red!9g!10-Green!11!12-Red!13g!14!15-Red!16!17"
		state3 = "Red!0g!1-Yellow!2!3-Red!4g!5!6-Red!7!8-Red!9g!10-Yellow!11!12-Red!13g!14!15-Red!16!17"
		state4 = "Red!0g!1-Red!2!3-Green!4g!5!6-Red!7!8-Red!9g!10-Red!11!12-Green!13g!14!15-Red!16!17"
		state5 = "Red!0g!1-Red!2!3-Yellow!4g!5!6-Red!7!8-Red!9g!10-Red!11!12-Yellow!13g!14!15-Red!16!17"
		state6 = "Red!0g!1-Red!2!3-Red!4g!5!6-Green!7!8-Red!9g!10-Red!11!12-Red!13g!14!15-Green!16!17"
		state7 = "Red!0g!1-Red!2!3-Red!4g!5!6-Yellow!7!8-Red!9g!10-Red!11!12-Red!13g!14!15-Yellow!16!17"
	'''
	duration_green = 10
	duration_yellow = 2
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state0, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state1, duration_yellow)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state2, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state3, duration_yellow)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state4, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state5, duration_yellow)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state6, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state7, duration_yellow)
	
	
def createJunction1_6c_TrafficData(junction_id, simulation_id):
		
	
	state0 = "Green!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Green!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state1 = "Yellow!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Yellow!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state2 = "Red!0g!1!2-Green!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Green!12!13-Red!14g!15-Red!16!17"
	state3 = "Red!0g!1!2-Yellow!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Yellow!12!13-Red!14g!15-Red!16!17"
	#state4 = "Red!0g!1!2-Red!3!4-Green!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Green!14g!15-Red!16!17"
	#state5 = "Red!0g!1!2-Red!3!4-Yellow!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Yellow!14g!15-Red!16!17"
	#state6 = "Red!0g!1!2-Red!3!4-Red!5g!6-Green!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Green!16!17"
	#state7 = "Red!0g!1!2-Red!3!4-Red!5g!6-Yellow!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Yellow!16!17"
	
	duration_green = 10
	duration_yellow = 2
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state0, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state1, duration_yellow)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state2, duration_green)
	TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state3, duration_yellow)
	#duration_green = 0
	#duration_yellow = 0
	#TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state4, duration_green)
	#TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state5, duration_yellow)
	#TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state6, duration_green)
	#TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id, state7, duration_yellow)