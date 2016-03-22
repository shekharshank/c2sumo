from DBUtil import *
		
##### junction - 202601366
def createjunction1Data(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,    
	    "west_lane_out":"51067022#17",
	    "west_lane_out_values":["51067022#17_0", "51067022#17_1"],
	    "west_lane_in":"-51067022#17",
	    "west_lane_in_values":["-51067022#17_1","-51067022#17_0"],
	    # left, straight, right
	    "west_lane_in_adjascent":["19493531#0", "-51067022#16", "19503247#0"],
	    
	    "east_lane_in":"51067022#16",
	    "east_lane_in_values":["51067022#16_0","51067022#16_1", "51067022#15_0","51067022#15_1"],
	    "east_lane_in_adjascent":["19503247#0", "51067022#17", "19493531#0"],
	    "east_lane_out":"-51067022#16",
	    "east_lane_out_values":["-51067022#16_1", "-51067022#16_0"],
	    
	    "north_lane_in":"-19493531#0",
	    "north_lane_in_values":["-19493531#0_0","-19493531#0_1"],
	    "north_lane_in_adjascent":["-51067022#16", "19503247#0", "51067022#17"],
	    "north_lane_out":"19493531#0",
	    "north_lane_out_values":["19493531#0_1","19493531#0_0"],
	    
	    "south_lane_out":"19503247#0",
	    "south_lane_out_values":["19503247#0_0","19503247#0_1"],
	    "south_lane_in":"-19503247#0",
	    "south_lane_in_values":["-19503247#0_1", "-19503247#0_1"],
	    "south_lane_in_adjascent":["51067022#17", "19493531#0", "-51067022#16"]
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#17_0_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#17_1_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#17_1_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#17_0_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#16_0_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#16_1_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#16_1_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#16_0_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19493531#0_0_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19493531#0_1_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19493531#0_1_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19493531#0_0_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19503247#0_0_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19503247#0_1_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19503247#0_1_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19503247#0_0_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})  
	
def createJunction1TurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# First intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-51067022#17",
		"left_turn": "0.2",		
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19493531#0", 
		"to_edge_straight": "-51067022#16",
		"to_edge_right": "19503247#0"
		
	})
	
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "51067022#16",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19503247#0", 
		"to_edge_straight": "51067022#17",
		"to_edge_right":  "19493531#0"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19493531#0",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-51067022#16", 
		"to_edge_straight": "19503247#0", 
		"to_edge_right": "51067022#17"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19503247#0",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "51067022#17", 
		"to_edge_straight": "19493531#0", 
		"to_edge_right": "-51067022#16"
		
	})
	
def createJunction1FlowData(intersection_id1, simulation_id):
	
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
	    "point_name": "A",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "-51067022#17",
		"to_edge_id": "n/a",
		"via_edge_id": "-51067022#17",
	    "flow_rate": "600",
		"latitude": "35.03940051840894",
		"longitude": "-85.2841266989708",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "B",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "-19493531#0",
		"to_edge_id": "n/a",
		"via_edge_id": "-19493531#0",
	    "flow_rate": "600",
		"latitude": "35.03974530201119",
		"longitude": "-85.28242081403732",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Iwest",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "51067022#11",
		"to_edge_id": "n/a",
		"via_edge_id": "51067022#11",
	    "flow_rate": "600",
		"latitude": "35.03683106612615",
		"longitude": "-85.27720928192139",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Ieast",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "-51067022#11",
		"to_edge_id": "n/a",
		"via_edge_id": "-51067022#11",
	    "flow_rate": "600",
		"latitude": "35.03701115003017",
		"longitude": "-85.27769207954407",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "Lnorth",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "-19503247#1",
		"to_edge_id": "n/a",
		"via_edge_id": "-19503247#1",
	    "flow_rate": "600",
		"latitude": "35.037911563597234",
		"longitude": "-85.28341591358185",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Lsouth",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id1,
		"from_edge_id": "19503247#1",
		"to_edge_id": "n/a",
		"via_edge_id": "19503247#1",
	    "flow_rate": "600",
		"latitude": "35.037485515487546",
		"longitude": "-85.28366267681122",
		"removable": "1"
	})