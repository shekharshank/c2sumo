from DBUtil import *
		
##### junction - 202666904
def createjunction3Data(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,    
	    "west_lane_out":"-19501516#11",
	    "west_lane_out_values":["-19501516#11_0", "-19501516#11_1"],
	    "west_lane_in":"19501516#11",
	    "west_lane_in_values":["19501516#11_1","19501516#11_0"],
	    # left, straight, right
	    "west_lane_in_adjascent":["-19514291#2", "19501516#12", "19514291#3"],
	    
	    "east_lane_in":"-19501516#12",
	    "east_lane_in_values":["-19501516#12_0","-19501516#12_1"],
	    "east_lane_in_adjascent":["19514291#3", "-19501516#11", "-19514291#2"],
	    "east_lane_out":"19501516#12",
	    "east_lane_out_values":["19501516#12_1", "19501516#12_0"],
	    
	    "north_lane_in":"19514291#2",
	    "north_lane_in_values":["19514291#2_0","19514291#2_1"],
	    "north_lane_in_adjascent":["19501516#12", "19514291#3", "-19501516#11"],
	    "north_lane_out":"-19514291#2",
	    "north_lane_out_values":["-19514291#2_1","-19514291#2_0"],
	    
	    "south_lane_out":"19514291#3",
	    "south_lane_out_values":["19514291#3_0","19514291#3_1"],
	    "south_lane_in":"-19514291#3",
	    "south_lane_in_values":["-19514291#3_1", "-19514291#3_0"],
	    "south_lane_in_adjascent":["-19501516#11", "-19514291#2", "19501516#12"]
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#11_0_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#11_1_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#11_1_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#11_0_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#12_0_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19501516#12_1_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#12_1_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19501516#12_0_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19514291#2_0_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "19514291#2_1_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	
	db.inductionloop.insert({
	    "_id": "-19514291#2_1_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514291#2_0_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19514291#3_0_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19514291#3_1_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514291#3_1_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514291#3_0_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})  
	
def createJunction3TurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# First intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "19501516#11",
		"left_turn": "0.2",		
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-19514291#2", 
		"to_edge_straight": "19501516#12",
		"to_edge_right": "19514291#3"
		
	})
	
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19501516#12",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19514291#3", 
		"to_edge_straight": "-19501516#11",
		"to_edge_right":  "-19514291#2"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "19514291#2",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19501516#12", 
		"to_edge_straight": "19514291#3", 
		"to_edge_right": "-19501516#11"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19514291#3",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-19501516#11", 
		"to_edge_straight": "-19514291#2", 
		"to_edge_right": "19501516#12"
		
	})
	
def createJunction3FlowData(junction_id, simulation_id):
	
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
	    "point_name": "Jsouth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19514291#2",
		"to_edge_id": "n/a",
		"via_edge_id": "19514291#2",
	    "flow_rate": "600",
		"latitude": "35.0339935941766",
		"longitude": "-85.27231693267822",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Jnorth",
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
	    "point_name": "E",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19501516#12",
		"to_edge_id": "n/a",
		"via_edge_id": "-19501516#12",
	    "flow_rate": "600",
		"latitude": "35.03179734081907",
		"longitude": "-85.27172684669495",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "F",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19514291#3",
		"to_edge_id": "n/a",
		"via_edge_id": "-19514291#3",
	    "flow_rate": "600",
		"latitude": "35.031525001289786",
		"longitude": "-85.27367949485779",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Kwest",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19457616#4",
		"to_edge_id": "n/a",
		"via_edge_id": "-19457616#4",
	    "flow_rate": "600",
		"latitude": "36.138208",
		"longitude": "-86.810903",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "Keast",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19501516#11",
		"to_edge_id": "n/a",
		"via_edge_id": "19501516#11",
	    "flow_rate": "600",
		"latitude": "35.034424052917046",
		"longitude": "-85.27881860733032",
		"removable": "0"
	})