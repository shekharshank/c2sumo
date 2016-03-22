from datetime import datetime
from DBUtil import *
import logging 

# We should be able to deconstruct all the steps that user has performed
# Individual Mode:
# Whenever User runs a simulation, we save the current simulation with status
# INACTIVE and backup time
# Whenever saves a simulation, we save the current simulation with status
# SAVED and backup time
# Whenever user copies an old simulation, we make an entry with current simulation 
# with status COPIED and backup time
# Whenever user deletes a saved simulation, we change the simulation 
# with status DELETED and add deleted time and deleted by userid

# for INACTIVE and COPIED
def saveSimulationData(simulation_id, status, user_id, from_id=None):
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one({
    		"_id": simulation_id,
	}, { "_id": 0 })
	
	# STATUS must be COPIED
	if from_id is not  None:
		sim_record['copied_from'] = from_id
	
	sim_record['status'] = status
	sim_record['backuptime'] = datetime.now()
	sim_record['old_id'] = simulation_id
	sim_record['backedup_by'] = user_id
	id = db.simulation.insert(sim_record)
	return id	

# make current	
def restoreSimulation(sim_id, problem_id, junction_id=None, lat=None, long=None):
	
	db = DBUtil().getDatabase()
	logging.info('Source simulation: ' + str(source_id) + ' has exec id') 
	setvar = {		    'duration': 0,
				    'running': False,
				    'problem_id': problem_id,
				    'current_exec_id': sim_record.get('current_exec_id')
				}
	if junction_id is not None:
		setvar[junctions] = [junction_id]
	if lat is not None:
		setvar[latitude] = lat
		setvar[longitude] = long
	db.simulation.update(
			{ "_id": sim_id },
			{ '$set': setvar,
			  '$unset': {'current_exec_id':""}
			}
		)

# make current	
def copySimulation(destination_id, source_id):
	
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one({
    		"_id": source_id
	}, { "_id": 0 })
	if sim_record.get('current_exec_id') is not None:
		logging.info('Source simulation: ' + str(source_id) + ' has exec id') 
		db.simulation.update(
			{ "_id": destination_id },
			{ '$set': {
				    'duration': 0,
				    'running': False,
				    'problem_id': sim_record['problem_id'],
				    'current_exec_id': sim_record.get('current_exec_id')
				}
			}
		)
	else:
		logging.info('Source simulation: ' + str(source_id) + ' does not have exec id, unsetting it in destination: ' + str(destination_id) ) 
		db.simulation.update(
			{ "_id": destination_id },
			{ '$set': {
				    'running': False,
				    'problem_id': sim_record['problem_id']
				   },
			  '$unset': {'current_exec_id':""}
			}
		)

# SAVE	
def saveSimulationDataWithUserInput(simulation_id, label, shared_with, user_id):
	db = DBUtil().getDatabase()
	existingLabel = db.simulation.find_one({
	    		"label": label, "status": "SAVED",
	})
	
	if existingLabel is not None:
		return {'response' : {'status': "exists"}}
		
	
	sim_record = db.simulation.find_one({
    		"_id": simulation_id,
	}, { "_id": 0 })
	
	sim_record['shared_with'] = shared_with
	if shared_with == 'NONE':
		sim_record['user_id'] = user_id
	elif shared_with == 'ALL':
		colab_group_record = db.studentgroup.find_one({
		    		"_id": sim_record['group_id'],
			}, { "_id": 0, "colab_group_id": 1 })
		sim_record["colab_group_id"] = colab_group_record["colab_group_id"]
		
	sim_record['status'] = 'SAVED'
	sim_record['label'] = label	
	sim_record['backuptime'] = datetime.now()
	sim_record['old_id'] = simulation_id
	sim_record['backedup_by'] = user_id
	id = db.simulation.insert(sim_record)
	return {'response' : {'status': "success", 'id':id}}

# DELETE
def deleteSimulation(sim_id, user_id):
	
	db = DBUtil().getDatabase()
	db.simulation.update(
		{ "_id": sim_id },
	        { '$set': {
	                    'status': 'DELETED',
	                    'delete_time': datetime.now(),
	                    'deleted_by': user_id
	                }
	        }
        )

# VERIFY COPY
def verifyCopySimulation(sim_id):
	
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one(
		{ "_id": sim_id }
        	)
	if sim_record['shared_with'] == 'ALL':
		return {'response' :  "ALL_MODE_COPY_FAILURE"}
	else:
		return None

def updateVehicleData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	db.vehicle.remove( { "simulation_id": simulation_id } )
	
	vehicle_cursor = db.vehicle.find({
    		"simulation_id": new_simid} , { "_id": 0 })
	
	for record in vehicle_cursor:
		record["simulation_id"] = simulation_id
		db.vehicle.insert(record)
		
def saveVehicleData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	vehicle_cursor = db.vehicle.find({
    		"simulation_id": simulation_id,
	}, { "_id": 0 })
	
	for record in vehicle_cursor:
		record['simulation_id'] = new_simid
		db.vehicle.insert(record)

def updateTurnProbData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	db.turnprobability.remove( { "simulation_id": simulation_id } )
	
	data = db.turnprobability.find({
    		"simulation_id": new_simid} , { "_id": 0 })
	
	for record in data:
		record["simulation_id"] = simulation_id
		db.turnprobability.insert(record)
		
def saveTurnProbData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	data = db.turnprobability.find({
    		"simulation_id": simulation_id,
	}, { "_id": 0 })
	
	for record in data:
		record['simulation_id'] = new_simid
		db.turnprobability.insert(record)

def updateFlowsData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	db.flows.remove( { "simulation_id": simulation_id } )
	
	data = db.flows.find({
    		"simulation_id": new_simid} , { "_id": 0 })
	
	for record in data:
		record["simulation_id"] = simulation_id
		db.flows.insert(record)
		
def saveFlowsData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	data = db.flows.find({
    		"simulation_id": simulation_id,
	}, { "_id": 0 })
	
	for record in data:
		record['simulation_id'] = new_simid
		db.flows.insert(record)

def updateTrafficLightLogicData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	db.trafficlightlogic.remove( { "simulation_id": simulation_id } )
	
	data = db.trafficlightlogic.find({
    		"simulation_id": new_simid} , { "_id": 0 })
	
	for record in data:
		record["simulation_id"] = simulation_id
		db.trafficlightlogic.insert(record)
		
def saveTrafficLightLogicData(simulation_id, new_simid):
	db = DBUtil().getDatabase()
	data = db.trafficlightlogic.find({
    		"simulation_id": simulation_id,
	}, { "_id": 0 })
	
	for record in data:
		record['simulation_id'] = new_simid
		db.trafficlightlogic.insert(record)
		
	
