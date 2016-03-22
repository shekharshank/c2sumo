from DBUtil import *
		
##### junction - 202666877
def createjunction4Data(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,    
	    "west_lane_out":"-19501516#2",
	    "west_lane_out_values":["-19501516#2_0", "-19501516#2_1"],
	    "west_lane_in":"19501516#2",
	    "west_lane_in_values":["19501516#2_1","19501516#2_0"],
	    # left, straight, right
	    "west_lane_in_adjascent":["-19503247#2", "19501516#3", "19503247#3"],
	    
	    "east_lane_in":"-19501516#3",
	    "east_lane_in_values":["-19501516#3_0","-19501516#3_1", "-19501516#4_0","-19501516#4_1"],
	    "east_lane_in_adjascent":["19503247#3", "-19501516#2", "-19503247#2"],
	    "east_lane_out":"19501516#3",
	    "east_lane_out_values":["19501516#3_1", "19501516#3_0"],
	    
	    "north_lane_in":"19503247#2",
	    "north_lane_in_values":["19503247#2_0","19503247#2_1"],
	    "north_lane_in_adjascent":["19501516#3", "19503247#3", "-19501516#2"],
	    "north_lane_out":"-19503247#2",
	    "north_lane_out_values":["-19503247#2_1","-19503247#2_0"],
	    
	    "south_lane_out":"19503247#3",
	    "south_lane_out_values":["19503247#3_0","19503247#3_1"],
	    "south_lane_in":"-19503247#3",
	    "south_lane_in_values":["-19503247#3_1", "-19503247#3_0"],
	    "south_lane_in_adjascent":["-19501516#2", "-19503247#2", "19501516#3"]
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#2_0_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#2_1_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#2_1_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#2_0_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#3_0_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#3_1_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#3_1_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#3_0_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19503247#2_0_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19503247#2_1_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	
	db.inductionloop.insert({
	    "_id": "-19503247#2_1_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19503247#2_0_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19503247#3_0_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19503247#3_1_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19503247#3_1_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19503247#3_0_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})  
	
def createJunction4TurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# First intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "19501516#2",
		"left_turn": "0.2",		
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-19503247#2", 
		"to_edge_straight": "19501516#3",
		"to_edge_right": "19503247#3"
		
	})
	
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19501516#3",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19503247#3", 
		"to_edge_straight": "-19501516#2",
		"to_edge_right":  "-19503247#2"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "19503247#2",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19501516#3", 
		"to_edge_straight": "19503247#3", 
		"to_edge_right": "-19501516#2"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19503247#3",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-19501516#2", 
		"to_edge_straight": "-19503247#2", 
		"to_edge_right": "19501516#3"
		
	})
	
def createJunction4FlowData(junction_id, simulation_id):
	
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
	db = connection.c3stem_database
	db.flows.insert({
	    "point_name": "Lsouth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19503247#2",
		"to_edge_id": "n/a",
		"via_edge_id": "19503247#2",
	    "flow_rate": "600",
		"latitude": "35.037911563597234",
		"longitude": "-85.28341591358185",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Lnorth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19457616#3",
		"to_edge_id": "n/a",
		"via_edge_id": "19457616#3",
	    "flow_rate": "600",
		"latitude": "36.139572",
		"longitude": "-86.810",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "Kwest",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19501516#4",
		"to_edge_id": "n/a",
		"via_edge_id": "-19501516#4",
	    "flow_rate": "600",
		"latitude": "35.034424052917046",
		"longitude": "-85.27881860733032",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Keast",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19456179#0",
		"to_edge_id": "n/a",
		"via_edge_id": "-19456179#0",
	    "flow_rate": "600",
		"latitude": "36.137900",
		"longitude": "-86.803260",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "G",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19503247#3",
		"to_edge_id": "n/a",
		"via_edge_id": "-19503247#3",
	    "flow_rate": "600",
		"latitude": "35.03558364477337",
		"longitude": "-85.28470873832703",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "H",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19501516#2",
		"to_edge_id": "n/a",
		"via_edge_id": "19501516#2",
	    "flow_rate": "600",
		"latitude": "35.037077034286156",
		"longitude": "-85.28547048568726",
		"removable": "0"
	})