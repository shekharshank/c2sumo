from datetime import datetime
from bson.objectid import ObjectId
from DBUtil import *
from DAO import TrafficLightDAO

def createGroupSimData(group_id, master_grp_id, latitude, longitude, mode, default_prob, junction_id_list, sim_id_list=None):	
	db = DBUtil().getDatabase()
	simulation_id = db.simulation.insert({
	    "group_id": group_id,
	    "master_group_id": master_grp_id,
	    "duration": 300,
	    "junctions":junction_id_list,
	    "latitude": latitude,
	    "longitude": longitude,
	    "update_rate": 2,
	    "max_update_size": 50,
	    "step_duration": 0.5,
	    "mode": mode,
	    "problem_id":default_prob,
	    "status": "ACTIVE",
	    "run_status": "NOT_RUNNING"
	})
	
	
	if sim_id_list is None:
		db.simulation_association.insert({
		    "sim_id" : simulation_id,
		    "sim_asso" : [simulation_id]
		})
		
		state0 = "Green!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Green!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
		state1 = "Yellow!0g!1!2-Red!3!4-Red!5g!6-Red!7!8-Yellow!9g!10!11-Red!12!13-Red!14g!15-Red!16!17"
		state2 = "Red!0g!1!2-Green!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Green!12!13-Red!14g!15-Red!16!17"
		state3 = "Red!0g!1!2-Yellow!3!4-Red!5g!6-Red!7!8-Red!9g!10!11-Yellow!12!13-Red!14g!15-Red!16!17"
		state4 = "Red!0g!1!2-Red!3!4-Green!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Green!14g!15-Red!16!17"
		state5 = "Red!0g!1!2-Red!3!4-Yellow!5g!6-Red!7!8-Red!9g!10!11-Red!12!13-Yellow!14g!15-Red!16!17"
		state6 = "Red!0g!1!2-Red!3!4-Red!5g!6-Green!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Green!16!17"
		state7 = "Red!0g!1!2-Red!3!4-Red!5g!6-Yellow!7!8-Red!9g!10!11-Red!12!13-Red!14g!15-Yellow!16!17"
	
		if junction_id_list[0] == "202601366":
			state0 = "Green!0g!1-Red!2!3-Red!4g!5!6-Red!7!8-Green!9g!10-Red!11!12-Red!13g!14!15-Red!16!17"
			state1 = "Yellow!0g!1-Red!2!3-Red!4g!5!6-Red!7!8-Yellow!9g!10-Red!11!12-Red!13g!14!15-Red!16!17"
			state2 = "Red!0g!1-Green!2!3-Red!4g!5!6-Red!7!8-Red!9g!10-Green!11!12-Red!13g!14!15-Red!16!17"
			state3 = "Red!0g!1-Yellow!2!3-Red!4g!5!6-Red!7!8-Red!9g!10-Yellow!11!12-Red!13g!14!15-Red!16!17"
			state4 = "Red!0g!1-Red!2!3-Green!4g!5!6-Red!7!8-Red!9g!10-Red!11!12-Green!13g!14!15-Red!16!17"
			state5 = "Red!0g!1-Red!2!3-Yellow!4g!5!6-Red!7!8-Red!9g!10-Red!11!12-Yellow!13g!14!15-Red!16!17"
			state6 = "Red!0g!1-Red!2!3-Red!4g!5!6-Green!7!8-Red!9g!10-Red!11!12-Red!13g!14!15-Green!16!17"
			state7 = "Red!0g!1-Red!2!3-Red!4g!5!6-Yellow!7!8-Red!9g!10-Red!11!12-Red!13g!14!15-Yellow!16!17"

		duration_green = 10
		duration_yellow = 2
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state0, duration_green)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state1, duration_yellow)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state2, duration_green)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state3, duration_yellow)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state4, duration_green)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state5, duration_yellow)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state6, duration_green)
		TrafficLightDAO().createTrafficLightLogic(simulation_id, junction_id_list[0], state7, duration_yellow)
		
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

	else:
		db.simulation_association.insert({
		    "sim_id" : simulation_id,
		    "sim_asso" : sim_id_list,
		    "is_master" : True
		})
	
	
	return simulation_id
