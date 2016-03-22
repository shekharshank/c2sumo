from SimulationDAO import getSimulationResultAllVehicles
from UserDataDAO import *
from DAO import *
from constants import GENERATED_DATA_FOLDER, TRAFFIC_SIGNAL_DATA_XML, STOP_SIGN_DATA_XML, TRAFFIC_DATA_XML
import sys
import shutil
import logging

class UserDataInterface (object):
		
	def getSimulationUserData(self, sim_id):
		return  getSimulationDataDB(sim_id)
		
	def getGroupType(self, group_id):
		return  getGroupTypeDB(group_id)
		
	def getActiveSimulationExecId(self, sim_id):
		return getActiveSimulationExecIdDB(sim_id)
		
	def getSimulationStepStatus(self, sim_id):
		return getSimulationStatusRecordDB(sim_id)
		
	def updateSimulationRunningState(self, sim_id, state, vmIP=None):
		updateSimulationRunningStateDB(sim_id, state, vmIP)
		
	def getSimulationRunningState(self, sim_id):
		return getSimulationRunningStateDB(sim_id)
	
	def finishSimulation(self, sim_id):
		finishSimulationDB(sim_id)
		
	def speedUpSimulationStepRate(self, sim_id):
		setMaxSimulationStepRateDB(sim_id)
		
	def restoreSimulationStepRate(self, sim_id):
		setDefaultSimulationStepRateDB(sim_id)
		
	def updateSimulationStepRate(self, sim_id, steps):
		updateSimulationStepRateDB(sim_id, steps)
	
	def getSimulationid(self, user_id):
		return getSimulationidDB(user_id)
		
	def getSimulationFolder(self, simulationID, user_id):
		user_mode = getUserModeDB(user_id)
		if user_mode != 'COLAB':
			return simulationID
		# user mode is COLAB, now check if it also the master
		if self.checkIfMasterSim(simulationID):
			return simulationID
		return SimAssociationDAO().getOrigIdForAssociatedSimID(simulationID)
		
	def setUserMode(self, user_id, mode):
		setUserModeDB(user_id, mode)
		
	def checkIfMasterSim(self, simulation_id):
		return checkIfMasterSimDB(simulation_id)
		
	def getUserDetails(self, user_id):
		return getUserDetailsDB(user_id)
		
	def getUserMode(self, user_id):
		return getUserModeDB(user_id)
		
	def ensureSimulationExists(self, simulationID):
		datafolder = GENERATED_DATA_FOLDER + str(simulationID)
		trafficSignalDataTemplate = GENERATED_DATA_FOLDER + TRAFFIC_SIGNAL_DATA_XML
		stopSignDataTemplate = GENERATED_DATA_FOLDER + STOP_SIGN_DATA_XML
		trafficDataTemplate = GENERATED_DATA_FOLDER + TRAFFIC_DATA_XML
		if not os.path.exists(datafolder):
			logging.info('Creating directory: ' + datafolder + ' for simulation: ' + str(simulationID));
			os.makedirs(datafolder)
			shutil.copy(trafficSignalDataTemplate, datafolder)
			shutil.copy(stopSignDataTemplate, datafolder)
			shutil.copy(trafficDataTemplate, datafolder)

	# In collaboration mode, UI needs to know the junctions
	# that a group is in collaboration with
	def getJunctionsofOtherGroups(self, simulationID, user_id):
		user_mode = getUserModeDB(user_id)
		junctions = []
		if user_mode == 'COLAB':
			#logging.info('simulationID ' + str(simulationID))
			#master_id = SimAssociationDAO().getOrigIdForAssociatedSimID(simulationID)
			#logging.info('master_id ' + str(master_id))
			#assoc_sim_ids = SimAssociationDAO().readAssociatedSimIDs(master_id)			
			#logging.info('assoc sim id ' + str(assoc_sim_ids))
			junctions = SimulationDAO().getRestOfIntersectionIDs(simulationID)

		return junctions
		
	def getSimulationHistoryTable(self, user_id):
		results = getSimulationHistoryTableDB(user_id)
		rows = []
		for result in results:
			cell = []		
			
			cell.append(result['label'])
			cell.append(result['problem_id'])
			if(result.get('user_id') is None):
				cell.append('-')
			else:
				cell.append(result['user_id'])
			cell.append(result['master_group_id'])
			cell.append(result['mode'])
			cell.append(result['shared_with'])
			if result.get('current_exec_id') is None:
				cell.append('-')
				cell.append('-')
				cell.append('-')
			else:
				exec_result = getSimulationResultAllVehicles(result['current_exec_id'])
				logging.info('Exec result : ' + str(exec_result));
				if len(exec_result['result']) == 0:
					cell.append('-')
					cell.append('-')
					cell.append('-')
				else:
					cell.append(exec_result['result'][0]['count'])
					cell.append(round(exec_result['result'][0]['speed'],2))
					cell.append(round(exec_result['result'][0]['waittime'],2))
			cell.append(str(result['backuptime']))
			row = {"id":str(result["_id"]), "cell": cell}
			rows.append(row)		
		return rows