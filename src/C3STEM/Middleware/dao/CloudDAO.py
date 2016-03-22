from bson.objectid import ObjectId
from DBUtil import *
import datetime
import logging

def getVMDetails(vm_name):
	db = DBUtil().getDatabase()
	vm_details = db.virtualmachine.find_one({
    		"_id": vm_name});
	return vmd_details
	
def getAssignedVMDetailsForSimID(sim_id):
	db = DBUtil().getDatabase()
	vm_details = db.virtualmachine.find_one({
    		"simulation_id": sim_id, "status": 'IN_USE'});
	return vm_details
	
def createVMRecord(vm):
	
	db = DBUtil().getDatabase()
	db.virtualmachine.insert({
	    "_id": vm.name,
	    "image_name": vm.image,
	    "flavor": vm.flavor,
	    "public_IP": vm.public_IP,
	    "private_IP": vm.private_IP,
	    "key_name": vm.key_name,
	    "type": vm.type,
	    "user": vm.user,
	    "vm_id": vm.id,
	    "host_id": vm.host_id
		});
		

def deleteVMRecord(id):
	db = DBUtil().getDatabase();
	return db.virtualmachine.remove({ "_id" : id});
	
def freeVMForSimID(sim_id):
	db = DBUtil().getDatabase();
	db.virtualmachine.update(
			{'simulation_id': sim_id},
				{  '$set':
					{ 'status': 'AVAIL'}
				} 
		)

def assignVMForSimulationID(sim_id):
	db = DBUtil().getDatabase();
	vm_details = db.virtualmachine.find_one({
    		"status": 'AVAIL'});
    	present_time = datetime.datetime.now()
    	if vm_details is None:
    		# no VM is available, check for stale VM
    		logging.info('no VM is available, checking for stale VM...');
    		stale_time =  present_time - datetime.timedelta(minutes=15)
    		vm_details = db.virtualmachine.find_one({
    			"sim_start_time": { "$lt": stale_time}, 'status': 'IN_USE'});    		
    	if vm_details is not None:
    		logging.info('Found Usable VM with ID: ' + vm_details["_id"]);
		db.virtualmachine.update(
		{'_id': vm_details["_id"]},
			{  '$set':
				{ 'status': 'IN_USE', 'simulation_id':ObjectId(sim_id), 'sim_start_time': present_time
			} 
		})
		vm_details = getAssignedVMDetailsForSimID(sim_id)
	return vm_details
