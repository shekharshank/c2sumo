from bson.objectid import ObjectId
from DBUtil import *
import ConfigParser
import os.path
import logging
from SimulationBackupInterface import *

def getSimulationDataDB(simulation_id):
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one({
    		"_id": simulation_id,
	}, { "longitude": 1, "latitude": 1, "group_id": 1, "junctions": 1, "mode": 1, "status": 1, "problem_id": 1, "_id": 0 })
	return sim_record
	
def getGroupTypeDB(group_id):
	db = DBUtil().getDatabase()
	group_record = db.studentgroup.find_one({
    		"_id": group_id,
	}, { "group_type": 1 })
	return group_record["group_type"]
	
def getActiveSimulationExecIdDB(simulation_id_str):
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one({
    		"_id": ObjectId(simulation_id_str),
	}, { "current_exec_id": 1, "_id": 0 })	
	return sim_record.get("current_exec_id")
	
	
def getSimulationStatusRecordDB(simulation_id):
	db = DBUtil().getDatabase()
	sim_record = db.simulation.find_one({
    		"_id": simulation_id,
	}, { "update_rate": 1, "max_update_size": 1, "running": 1, "step_duration": 1 })
	return sim_record
	
def updateSimulationRunningStateDB(simulation_id, state, vmIP):	
	db = DBUtil().getDatabase()
	if(vmIP is None):
		db.simulation.update({
			    	"_id": simulation_id,
			}, { "$set": {"run_status": state} })
	else:
		db.simulation.update({
		    	"_id": simulation_id,
			}, { "$set": {"run_status": state, "vmIP":vmIP} })
		
def getSimulationRunningStateDB(sim_id):
	db = DBUtil().getDatabase()
	record = db.simulation.find_one({
    		"_id": sim_id
		}, { "run_status": 1 })
	return record["run_status"]	

def finishSimulationDB(simulation_id):
	db = DBUtil().getDatabase()
	db.simulation.update({
    			"_id": simulation_id,
		}, { "$set": {"running": False} })

def getUserDetailsDB(user_id):
	db = DBUtil().getDatabase()
	user_record = db.student.find_one({
    		"_id": user_id
	})
	if user_record is not None:
		group_record = db.studentgroup.find_one({
		    		"_id": user_record['group_id']		
		})
		user_record['groupname'] = group_record['name']
	return user_record
	
def updateSimulationStepRateDB(simulation_id, steps):
	db = DBUtil().getDatabase()
	
	if int(steps) > 0: 
		db.simulation.update({
    			"_id": simulation_id,
		}, { "$set": {"update_rate": int(steps)} })
		
def setMaxSimulationStepRateDB(simulation_id):
	db = DBUtil().getDatabase()
	db.simulation.update({
    			"_id": simulation_id,
		}, { "$set": {"update_rate": 0} })
		
		
def setDefaultSimulationStepRateDB(simulation_id):
	#move this a level UP
	conf = os.path.join(os.path.dirname(__file__), './config/application.conf')
	Config = ConfigParser.ConfigParser();
	Config.read(conf);
	steps = int(Config.get('SIMULATION', 'UPDATE_RATE'))
	db = DBUtil().getDatabase()
	db.simulation.update({
    			"_id": simulation_id,
		}, { "$set": {"update_rate": steps} })
		
def checkIfMasterSimDB(simulation_id):
	db = DBUtil().getDatabase()
	asso_records = db.simulation_association.find_one({ "sim_id": simulation_id} )
	return asso_records.get('is_master')
		
def getSimulationidDB(userid):
    #ignore the supplied mode field
    user_mode = getUserModeDB(userid);
    db = DBUtil().getDatabase()
    grouprow = db.student.find_one({"_id":userid})
    if grouprow is None:
	return None;

    if user_mode == 'INDIVIDUAL':
	simulation = db.simulation.find_one({"user_id":userid, "mode": user_mode, "status": "ACTIVE"})
	# simulation id for personal mode is not created yet
	# need to handle cases where there are no personal and group modes
	if simulation is None:
		group_simulation = db.simulation.find_one({"group_id":grouprow["group_id"], "mode": 'GROUP', "status": "ACTIVE"})
		#, { "_id": 0 })
		group_simulation['user_id'] = userid
		group_simulation['mode'] = user_mode
		groud_sim_id = group_simulation['_id']
		group_simulation['_id'] = ObjectId();
		simulation_id = db.simulation.insert(group_simulation)
		db.simulation_association.insert({   "sim_id" : simulation_id,
			"sim_asso" : [simulation_id]
			})
		interface = SimulationBackupInterface(groud_sim_id)
		interface.saveSimulationWithNewId(simulation_id);
		return simulation_id;
	return simulation["_id"]
    logging.info("For user " + userid + ", group_id is " + grouprow["group_id"])
    simulation = db.simulation.find_one({"group_id":grouprow["group_id"], "mode": user_mode, "status": "ACTIVE"})
    return simulation["_id"]


def setUserModeDB(userid, mode):
     db = DBUtil().getDatabase();
     print userid, mode
     user_details = getUserDetailsDB(userid)
     previous_mode = getUserModeDB(userid)
     db.user_studentrole.update(
		{ 'user_id': userid },
		{  '$set':
			{ 'user_mode': mode			
			} 
		}
     )
     # all this mess is for making collaboration within the same grp possible, 
     # revert if the project continues
     if (mode == 'COLAB' and previous_mode != 'COLAB'):
     	new_grp_id = user_details['group_id'] + '_' + user_details['colab_group_type'] + '_COLAB'
    	db.student.update(
     		{ '_id': userid },
     		{  '$set':
     			{ 'group_id': new_grp_id 			
     			} 
     		}
     	)
     	logging.info("For user " + userid + ", group_id set to " + new_grp_id)
     elif (mode != 'COLAB' and previous_mode == 'COLAB'):
     	id_parts = user_details['group_id'].split('_')
     	new_grp_id = id_parts[0] + '_' + id_parts[1]
        db.student.update(
		{ '_id': userid },
		{  '$set':
			{ 'group_id': new_grp_id 			
			} 
		}
     	)
     	logging.info("For user " + userid + ", group_id set to " + new_grp_id)

def getUserModeDB(userid):
     db = DBUtil().getDatabase();
     usermoderow = db.user_studentrole.find_one(
		{ 'user_id': userid })
     if usermoderow.get('user_mode') is None:
	return 'GROUP';
     else:
	return usermoderow['user_mode'];
	
def getSimulationHistoryTableDB(userid):
	db = DBUtil().getDatabase()
	grouprow = db.student.find_one({"_id":userid}, {'group_id' : 1, 'master_group_id' : 1})
	mode = getUserModeDB(userid)
	logging.info('group data: ' + str(grouprow))
	return db.simulation.find(
	 {'$and' :
	  [
	   { "status": 'SAVED'},
	   { "mode": mode},
           { '$or' :
		[
    	 		
    			 {'$and' : [ {"shared_with": 'GROUP'}, {"master_group_id": grouprow['master_group_id']} ]},
    			 {'$and' : [ {"shared_with": 'NONE'}, {"user_id": userid} ]}
    		]
           }
          ]
         });