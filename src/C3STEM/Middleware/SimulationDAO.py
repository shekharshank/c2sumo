from datetime import datetime
from bson.objectid import ObjectId
from DBUtil import *
		
def saveSimulationStart(simulation_id):
	db = DBUtil().getDatabase()
	sim_exec_id = db.simulation_run.insert({
    		"simstarttime": datetime.now()
		})
	db.simulation.update(
			{'_id': simulation_id},
				{  '$set':
					{ 'running': True, 'current_exec_id' : sim_exec_id}
				} 
		)
		
	db.virtualmachine.update(
				{'_id': simulation_id},
					{  '$set':
						{ 'sim_start_time': datetime.now()}
					} 
		)
	
	return sim_exec_id
	
def getSimulationData(simulation_id):
	db = DBUtil().getDatabase()
	sim_row = db.simulation.find_one({
		    		"_id": ObjectId(simulation_id)})
	if sim_row is None:
		return None
	else:
		return sim_row
	
	
def saveSimulationEnd(sim_exec_id, steps):
	db = DBUtil().getDatabase()
	db.simulation_run.update(
		{ '_id': sim_exec_id },
		{  '$set':
			{ 'simendtime': datetime.now(),
			  'steps': steps			
			} 
		}
	)
	
def getSimulationResultByVehicleType(sim_exec_id):
	db = DBUtil().getDatabase()
	vehicleResult = db.simulation_trip_data.aggregate([
		       
			  { "$match" : {"sim_exec_id" : ObjectId(sim_exec_id)}},
			   {"$group": {"_id": "$type", "count": {"$sum": 1}, "speed": {"$avg": "$speed"}, "waittime": {"$sum": "$waittime"} }}
			   ])	
	return vehicleResult
	
def getSimulationResultAllVehicles(sim_exec_id):
	db = DBUtil().getDatabase()
	simResult = db.simulation_trip_data.aggregate([
		       
			  { "$match" : {"sim_exec_id" : ObjectId(sim_exec_id)}},
			   {"$group": {"_id": "null", "count": {"$sum": 1}, "speed": {"$avg": "$speed"}, "waittime": {"$avg": "$waittime"} }}
			   ])	
	return simResult
	
def getSimulationDuration(sim_exec_id):
	db = DBUtil().getDatabase()
	return db.simulation_run.find_one({
		    		"_id": ObjectId(sim_exec_id)}, { "steps": 1, "_id": 0})
	
	
def getSimulationInducionLoopResult(sim_exec_id, loop_id):
	db = DBUtil().getDatabase()
	#Loop id is junction + location, get the associated loop id based on this
	idparts = loop_id.split("!")	
	inductionloops = db.inductionloop.find({"junction": idparts[1], "location": idparts[0]})
	id_search = [];
	for inductionloop in inductionloops:
		idkey = {}
		idkey['induction_id'] = inductionloop["_id"]	
		id_search.append(idkey)	
			
	queryResult = db.simulation_induction_data.find( {"sim_exec_id" : ObjectId(sim_exec_id), "$or": id_search} 
						, { "endtime": 1, "count": 1, "vehicletype": 1}
						).sort( "endtime", 1 )
	
	return queryResult
	
def getDistinctSimulatedVehicleList(sim_exec_id, loop_id):
	db = DBUtil().getDatabase()
	#Loop id is junction + location, get the associated loop id based on this
	idparts = loop_id.split("!")
	inductionloops = db.inductionloop.find({"junction": idparts[1], "location": idparts[0]})
	id_search = [];
	for inductionloop in inductionloops:
		idkey = {}
		idkey['induction_id'] = inductionloop["_id"]	
		id_search.append(idkey)	
				
	queryResult = db.simulation_induction_data.find( {"sim_exec_id" : ObjectId(sim_exec_id), "$or": id_search} 
							, { "vehicletype": 1}
							).distinct("vehicletype")		
	
	vehicle_types = []
	for vehicle in queryResult:
		vehicle_types.append(vehicle)
	return vehicle_types
	
	
def getSimulationQueueResult(sim_exec_id, loop_id):
	#Loop id is junction + location, get the associated lanes based on this
	db = DBUtil().getDatabase()
	idparts = loop_id.split("!")	
	lanes = db.junction.find_one({"_id": idparts[1]})[idparts[0] + "_values"]
	lane_search = [];
	for lane in lanes:
		lanekey = {}
		lanekey['lane_id'] = lane	
		lane_search.append(lanekey)

		
	queueResult = db.simulation_queue_data.aggregate([
		       
			   { "$match" : {"sim_exec_id" : ObjectId(sim_exec_id), 
			    "$or": lane_search} },
			   {"$group": {"_id": "$timestep", "queueinglength": {"$sum": "$queueinglength"} }}
			   ])
	return queueResult

def getAdjascentLanes(location, junction):
	db = DBUtil().getDatabase()	
	adjascent_routes_field = location + "_adjascent"
	return db.junction.find_one({"_id": junction}, { location: 1, adjascent_routes_field: 1, "_id":0})
	
def getSimulatedVehicleRoutes(sim_exec_id):
	db = DBUtil().getDatabase()
	data = db.simulation_trip_data.find({"sim_exec_id": ObjectId(sim_exec_id)}, {"route":1, "_id":0})
	return data

def getSimulationInductionFlowRate(sim_exec_id, location, junction):
	#Loop id is junction + location, get the associated lanes based on this
	db = DBUtil().getDatabase()	
	inductionloops = db.inductionloop.find({"junction": junction, "location": location})
	id_search = [];
	for inductionloop in inductionloops:
		idkey = {}
		idkey['induction_id'] = inductionloop["_id"]	
		id_search.append(idkey)	
	
	# get all data if out type
	result = db.simulation_induction_data.aggregate([
		       
			   { "$match" : {"sim_exec_id" : ObjectId(sim_exec_id), 
			    "$or": id_search} },
			    {"$group": { "_id" : "null", "vehiclecount": {"$sum": "$count"}, "endtime": {"$max": "$endtime"} }}
			   ])
	
	return result
	
def getSimulationInductionQueueResult(sim_exec_id, location, junction):
	#Loop id is junction + location, get the associated lanes based on this
	db = DBUtil().getDatabase()	
	lanes = db.junction.find_one({"_id": junction})[location + "_values"]
	lane_search = [];
	for lane in lanes:
		lanekey = {}
		lanekey['lane_id'] = lane	
		lane_search.append(lanekey)

		
	queueResult = db.simulation_queue_data.aggregate([
		       
			   { "$match" : {"sim_exec_id" : ObjectId(sim_exec_id), "queueinglength" : {"$ne" : 0},
			    "$or": lane_search} },
			   {"$group": {"_id": "null", "queueinglength": {"$avg": "$queueinglength"}, "maxqueueduration": {"$max": "$queueingtime"}, 
			   	"endtime": {"$max": "$timestep"}, "count": {"$sum": 1}, "totalqueueinglength": {"$sum": "$queueinglength"} }}
			   ])
	return queueResult
	
def getSimulationExecutionDetails(sim_exec_id):
	db = DBUtil().getDatabase()
	result = db.simulation_run.find_one({"_id": ObjectId(sim_exec_id)}, { "_id":0})
	return result
	
		