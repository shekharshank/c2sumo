from DBUtil import *
		
##### junction - 202662093
def createjunction2Data(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,    
	    "west_lane_out":"51067022#8",
	    "west_lane_out_values":["51067022#8_0", "51067022#8_1"],
	    "west_lane_in":"-51067022#8",
	    "west_lane_in_values":["-51067022#8_1","-51067022#8_0"],
	    # left, straight, right
	    "west_lane_in_adjascent":["19514446#0", "-51067022#7", "19514291#0"],
	    
	    "east_lane_in":"51067022#7",
	    "east_lane_in_values":["51067022#7_0","51067022#7_1"],
	    "east_lane_in_adjascent":["19514291#0", "51067022#8", "19514446#0"],
	    "east_lane_out":"-51067022#7",
	    "east_lane_out_values":["-51067022#7_1", "-51067022#7_0"],
	    
	    "north_lane_in":"-19514446#0",
	    "north_lane_in_values":["-19514446#0_0","-19514446#0_1"],
	    "north_lane_in_adjascent":["-51067022#7", "19514291#0", "51067022#8"],
	    "north_lane_out":"19514446#0",
	    "north_lane_out_values":["19514446#0_1","19514446#0_0"],
	    
	    "south_lane_out":"19514291#0",
	    "south_lane_out_values":["19514291#0_0","19514291#0_1"],
	    "south_lane_in":"-19514291#0",
	    "south_lane_in_values":["-19514291#0_1", "-19514291#0_0"],
	    "south_lane_in_adjascent":["51067022#8", "19514446#0", "-51067022#7"]
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#8_0_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#8_1_5",
	    "junction": junction_id,    
	    "location": "west_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#8_1_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#8_0_-5",
	    "junction": junction_id,    
	    "location": "west_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#7_0_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "51067022#7_1_-5",
	    "junction": junction_id,    
	    "location": "east_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#7_1_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-51067022#7_0_5",
	    "junction": junction_id,    
	    "location": "east_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514446#0_0_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514446#0_1_-5",
	    "junction": junction_id,    
	    "location": "north_lane_in",
	    "pos": -5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19514446#0_1_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19514446#0_0_5",
	    "junction": junction_id,    
	    "location": "north_lane_out",
	    "pos": 5
	})
	
	
	db.inductionloop.insert({
	    "_id": "19514291#0_0_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "19514291#0_1_5",
	    "junction": junction_id,    
	    "location": "south_lane_out",
	    "pos": 5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514291#0_1_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})
	
	db.inductionloop.insert({
	    "_id": "-19514291#0_0_-5",
	    "junction": junction_id,    
	    "location": "south_lane_in",
	    "pos": -5
	})  
	
def createJunction2TurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# First intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-51067022#8",
		"left_turn": "0.2",		
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19514446#0", 
		"to_edge_straight": "-51067022#7",
		"to_edge_right": "19514291#0"
		
	})
	
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "51067022#7",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "19514291#0", 
		"to_edge_straight": "51067022#8",
		"to_edge_right":  "19514446#0"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19514446#0",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "-51067022#7", 
		"to_edge_straight": "19514291#0", 
		"to_edge_right": "51067022#8"
		
	})	

	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
		"edge_id": "-19514291#0",
		"left_turn": "0.2",
		"go_straight": "0.6",
		"right_turn": "0.2",
		"to_edge_left": "51067022#8", 
		"to_edge_straight": "19514446#0", 
		"to_edge_right": "-51067022#7"
		
	})
	
def createJunction2FlowData(intersection_id2, simulation_id):
	
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
	    "point_name": "Iwest",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "51067022#11",
		"to_edge_id": "n/a",
		"via_edge_id": "51067022#11",
	    "flow_rate": "600",
		"latitude": "35.03683106612615",
		"longitude": "-85.27720928192139",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "C",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "-19514446#0",
		"to_edge_id": "n/a",
		"via_edge_id": "-19514446#0",
	    "flow_rate": "600",
		"latitude": "35.03563635310315",
		"longitude": "-85.27142643928528",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "D",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "51067022#7",
		"to_edge_id": "n/a",
		"via_edge_id": "51067022#7",
	    "flow_rate": "600",
		"latitude": "35.03441526806746",
		"longitude": "-85.27075052261353",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Jnorth",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "-19514291#1",
		"to_edge_id": "n/a",
		"via_edge_id": "-19514291#1",
	    "flow_rate": "600",
		"latitude": "35.0339935941766",
		"longitude": "-85.27231693267822",
		"removable": "0"
	})
	
	db.flows.insert({
	    "point_name": "Jsouth",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "19514291#1",
		"to_edge_id": "n/a",
		"via_edge_id": "19514291#1",
	    "flow_rate": "600",
		"latitude": "35.037911563597234",
		"longitude": "-85.28341591358185",
		"removable": "1"
	})
	
	db.flows.insert({
	    "point_name": "Ieast",
		"simulation_id": simulation_id,
	    "intersection_id": intersection_id2,
		"from_edge_id": "-51067022#11",
		"to_edge_id": "n/a",
		"via_edge_id": "-51067022#11",
	    "flow_rate": "600",
		"latitude": "35.0369716194511",
		"longitude": "-85.27770280838013",
		"removable": "0"
	})