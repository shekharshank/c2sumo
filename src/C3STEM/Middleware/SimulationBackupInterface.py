from SimulationBackupDAO import *
from bson.objectid import ObjectId
import logging

class SimulationBackupInterface (object):

	def __init__(self, sim_id):
		self.id = sim_id
		
	def backupSimulation(self, user_id, status):
		new_id = saveSimulationData(self.id, status, user_id)
		self.saveSimulationRelatedData(new_id)
		return new_id
	
	def copySimulationToCurrent(self, copied_id, user_id):
		sim_id = ObjectId(copied_id)
		# first check if the colab mode copy for all is not performed
		response = verifyCopySimulation(sim_id)
		if response is not None:
			return response;
		# take backup of current simulation
		# copied id is not in object id format
		new_id = saveSimulationData(self.id, 'COPIED', user_id, sim_id)
		self.saveSimulationRelatedData(new_id)
		
		# copy data from old to current simulation
		copySimulation(self.id, sim_id)
		self.updateSimulationRelatedData(sim_id)
		return None
		
	def deleteSimulation(self, user_id):
		deleteSimulation(self.id, user_id)
		
	def saveSimulationWithNewId(self, new_id):
		self.saveSimulationRelatedData(new_id)
	
	def saveSimulationWithUserInput(self, label, shared_with, user_id):
		logging.info('Accepted request to save simulation: ' + str(self.id) + ' with label: ' + label);
		response = saveSimulationDataWithUserInput(self.id, label, shared_with, user_id)
		if response['response']['status'] == 'success':
			self.saveSimulationRelatedData(response['response']['id'])
			return {'response' : {'status': "success"}}
		return response
	
	def updateSimulationRelatedData(self, new_id):
		updateVehicleData(self.id, new_id)
		updateTurnProbData(self.id, new_id)
		updateFlowsData(self.id, new_id)
		updateTrafficLightLogicData(self.id, new_id)
	
	def saveSimulationRelatedData(self, new_id):
		saveVehicleData(self.id, new_id)
		saveTurnProbData(self.id, new_id)
		saveFlowsData(self.id, new_id)
		saveTrafficLightLogicData(self.id, new_id)
		
		
		
	