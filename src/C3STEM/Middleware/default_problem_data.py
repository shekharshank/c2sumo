from DBUtil import *
from SimulationBackupInterface import *

def createDefaultVehicleData(simulation_id):
	connection = MongoClient()
	db = connection.c3stem_database
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

def setSimulationToDefault(sim_id, problem_id, intersection_id):
	default_id = "DEFAULT_" + problem_id + "_" + intersection_id
	updateTurnProbData(sim_id, default_id)
	updateFlowsData(sim_id, default_id)
	updateTrafficLightLogicData(sim_id, default_id)
	default_for_vehicle_id = "DEFAULT_" + problem_id
	updateVehicleData(sim_id, default_for_vehicle_id)
	
		
def createTurnProbability(sim_id, problem_id, intersection_id=None):
	default_id = "DEFAULT_" + problem_id + "_" + intersection_id
	updateTurnProbData(sim_id, default_id)
	
def createFlowData(sim_id, problem_id, intersection_id=None):
	default_id = "DEFAULT_" + problem_id + "_" + intersection_id
	updateFlowsData(sim_id, default_id)
	
def createTrafficLightData(sim_id, problem_id, intersection_id=None):
	default_id = "DEFAULT_" + problem_id + "_" + intersection_id
	updateTrafficLightLogicData(sim_id, default_id)
	
def createVehicleData(sim_id, problem_id):
	default_id = "DEFAULT_" + problem_id
	updateVehicleData(sim_id, default_id)
	
		