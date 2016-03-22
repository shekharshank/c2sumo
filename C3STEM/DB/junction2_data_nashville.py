from DBUtil import *
import sys
sys.path.insert(0, '/opt/C3STEM/Middleware')
from DAO import TrafficLightDAO
		
##### junction - 202305458
def createjunction2DefaultData(junction_id):
	connection = MongoClient()
	db = connection.c3stem_database
	db.junction.insert({
	    "_id": junction_id,
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
	
def createJunction2DefaultTurnProbability(junction_id, simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
	
	# Second intersection
	db.turnprobability.insert({
		"simulation_id": simulation_id, 
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
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
		"intersection_id": junction_id, 
		"edge_id": "-19463160#8",
		"left_turn": "0.3",
		"right_turn": "0.3",
		"go_straight": "0.4",
		"to_edge_left": "19456179#0",
		"to_edge_right": "-19479801#4",
		"to_edge_straight": "-19463160#7"
	})
	
def createJunction2DefaultFlowData(intersection_id2, simulation_id):
	
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
	
	junction_id = intersection_id2
	connection = MongoClient()
	db = connection.c3stem_database
	db.flows.insert({
		    "point_name": "C",
			"simulation_id": simulation_id,
		    "intersection_id": junction_id,
			"from_edge_id": "19463160#7",
			"to_edge_id": "n/a",
			"via_edge_id": "19463160#7",
		    "flow_rate": "600",
			"latitude":"36.138488",
			"longitude": "-86.800700",
			"removable": "0"
		})
		
	db.flows.insert({
	    "point_name": "D",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19479801#4",
		"to_edge_id": "n/a",
		"via_edge_id": "19479801#4",
	    "flow_rate": "600",
		"latitude": "36.137566",
		"longitude": "-86.799433",
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
		"removable": "1"
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
		"removable": "0"
	})

	db.flows.insert({
	    "point_name": "Jnorth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "-19463160#8",
		"to_edge_id": "n/a",
		"via_edge_id": "-19463160#8",
	    "flow_rate": "600",
		"latitude": "36.13587822736727",
		"longitude": "-86.80091857910156",
		"removable": "0"
	})

	db.flows.insert({
	    "point_name": "Jsouth",
		"simulation_id": simulation_id,
	    "intersection_id": junction_id,
		"from_edge_id": "19463160#8",
		"to_edge_id": "n/a",
		"via_edge_id": "19463160#8",
	    "flow_rate": "600",
		"latitude": "36.137234",
		"longitude": "-86.800842",
		"removable": "1"
	})
	
	
def createJunction2DefaultTrafficData(junction_id, simulation_id):

	state0 = "Green!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Green!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state1 = "Yellow!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Yellow!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
	state2 = "Red!0g!1!2-Green!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Green!12!13-Red!14g!15-Red!16!17"
	state3 = "Red!0g!1!2-Yellow!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Yellow!12!13-Red!14g!15-Red!16!17"
	state4 = "Red!0g!1!2-Red!3!4-Green!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Green!14g!15-Red!16!17"
	state5 = "Red!0g!1!2-Red!3!4-Yellow!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Yellow!14g!15-Red!16!17"
	state6 = "Red!0g!1!2-Red!3!4-Red!5g!6-Green!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Green!16!17"
	state7 = "Red!0g!1!2-Red!3!4-Red!5g!6-Yellow!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Yellow!16!17"

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
	
	
def createJunction2_6c_TrafficData(junction_id, simulation_id):
		
	
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