from xml.etree.ElementTree import  ElementTree, Element, SubElement
import threading
import logging


import pymongo
from pymongo import MongoClient

from constants import *
from DBUtil import *
from util import *
from UserDataInterface import *
from SimulationBackupInterface import *
from cloud.CloudInterface import *
from RemoteSimulation import *
from c3stemtraci import Traci
from SimulationDAO import *

class trafficsim(threading.Thread):

	cloudInterface = CloudInterface()
        	
	def __init__(self, vmIP, key_name, simulationID, userID):
		threading.Thread.__init__(self)
		logging.info('Initializing trafficsim from SimID' + str(simulationID));
		self.userInterface = UserDataInterface()
		self.simulationID = simulationID
		self.strSimulationID = str(simulationID)
		self.userID = userID
		self.vmIP = vmIP 
		self.keyName = key_name
		self.sumoport = SUMO_PORT
		datafolder = GENERATED_DATA_FOLDER + self.strSimulationID
		self.trafficdataxml = datafolder + TRAFFIC_DATA_XML
		self.trafficsignaldataxml = datafolder + TRAFFIC_SIGNAL_DATA_XML
		self.running = False
		self.traciinst = None
		print("..initialized")
			
	def disconnectSumo(self):
		print("Disconnecting...")
		self.running = False
		if self.traciinst is not None:
			self.traciinst.close()
		
		#self.sumoprocess.wait()
		
	def run(self):
		try:			
			print("running...")
			logging.info('Performing simulation setup for id: ' + self.strSimulationID);
			self.userInterface.updateSimulationRunningState(self.simulationID, "INITIALIZING", self.vmIP)
			self.remoteSim = RemoteSimulation(self.vmIP, self.keyName, self.simulationID)
			if (self.remoteSim.runRemoteSimulation() is False):
				logging.exception('Unable to start remote simulation: ' + self.strSimulationID)
				return		
			logging.info('Simulation server started for id: ' + self.strSimulationID);
			self.userInterface.ensureSimulationExists(self.simulationID)
			self.userInterface.updateSimulationRunningState(self.simulationID, "CONFIGURED")
    			# wait for simulation to start server
			time.sleep(1)
			logging.info('Instantiating Traci for host: ' + self.vmIP + ' for simulation: ' + self.strSimulationID);
			self.traciinst = Traci(port=self.sumoport, numRetries=10, host=self.vmIP)
			logging.info('Backing up old simulation...' + ' for simulation: ' + self.strSimulationID);
			backup = SimulationBackupInterface(self.simulationID)
			backup.backupSimulation(self.userID, "INACTIVE")
			
			logging.info('Starting simulation with ID: ' + self.strSimulationID);			
			self.mainloop()			
			
		except Exception, err:
			logging.exception(err)
		
		finally:
			self.userInterface.updateSimulationRunningState(self.simulationID, "NOT_RUNNING")
			logging.info('Finally disconnecting from SUMO')
			self.freeVM()
			self.disconnectSumo()
			
			
	def freeVM(self):
		self.cloudInterface.freeVMForSimulation(self.simulationID)
		
	def mainloop(self):
		print("main loop...")
		self.running = True
		logging.info('Saving simulation start info for simulation: ' + self.strSimulationID);
		sim_exec_id = saveSimulationStart(self.simulationID)	
		logging.info('Starting simulation with exec id:' + str(sim_exec_id));
		sim_time = 0;
		self.userInterface.updateSimulationRunningState(self.simulationID, "RUNNING")
		while True:
			record = self.userInterface.getSimulationStepStatus(self.simulationID)
			step_rate = record["update_rate"]
			step_size = record["max_update_size"]
			# default is 500 ms as step duration
			self.step_duration = record["step_duration"]
			self.running = record["running"]
			if self.running == False:
				logging.info('Stopping simulation: ' + self.strSimulationID);
				time.sleep(self.simsleepperiod)
				#self.finishSimulation()
				logging.info('Simulation ended, now performing bookeeping..')
				break
			
			if step_rate == 0:
				#logging.info('Simulation ' + self.strSimulationID + ' in fast mode..');
				self.simsleepperiod = 0;
				self.simulationStep(step_size)
			else:
				self.simsleepperiod = 1.0 / step_rate
				self.simulationStep(0)
			
			sim_time = self.getSimTime();
			# Task: Update positions in XML file			
			self.updatepositions(sim_exec_id, sim_time)
			
			# Task: Update traffic light status in XML file
			self.updatetrafficlights()						
			
			# Sleep little bit (second)
			time.sleep(self.simsleepperiod)
			
			if self.running == False:
				logging.info('Stopping simulation: ' + self.strSimulationID);
				time.sleep(self.simsleepperiod)
				#self.finishSimulation()
				logging.info('Simulation ended, now perform bookeeping..')
				break
				
			min_expected_vehicles = self.traciinst.simulation.getMinExpectedNumber()
			if min_expected_vehicles == 0 and sim_time > 10:
				break;
			
		logging.info('Updating simulation data for simulation: ' + self.strSimulationID);
		sim_time = self.getSimTime()
		logging.info('Simulation id: ' + str(self.simulationID) + ', Simulation Exec Id: ' + str(sim_exec_id) + ', sim_time: ' + str(sim_time) );
		self.updateSimulationEndData(sim_exec_id, sim_time)
		self.remoteSim.aggregateSimulationData(sim_exec_id)
		self.remoteSim.close()
		logging.info('Updated simulation data for simulation: ' + self.strSimulationID);
	
	def updateSimulationEndData(self, sim_exec_id, sim_time):
		saveSimulationEnd(sim_exec_id, sim_time)
		tree = ElementTree()
		tree.parse(self.trafficdataxml)
		item = tree.find('sim_execution')
		item.set('duration', str(sim_time))	
		item.set('running', 'false')	
		tree.write(self.trafficdataxml)
		
	def finishSimulation(self):
		# A very large value
		logging.info('Performing large simulation end time')
		self.traciinst.simulationStep(2000000)
		
	def restoreSimulationRate(self):		
		step_rate = self.userInterface.getSimulationStepRate(self.simulationID)
		self.simsleepperiod = 1.0 / step_rate	
		
		
			
	def updatepositions(self, sim_exec_id, sim_time):
		root = Element('trafficdata')		
		SubElement(root, 'sim_execution', {
		                              'duration':str(sim_time),
		                              'id': str(sim_exec_id),
		                              'running':'true',
		                              })		
		vehicles = SubElement(root, 'vehicles')
		
		#Get vehicle list
		vehiclelist = self.getpos()
		for veh in vehiclelist:
			type = veh[3].split("_")[0]
			SubElement(vehicles, 'vehicle', {
				                              'id': str(veh[0]),
				                              'lat': str(veh[1]),
				                              'lng': str(veh[2]),
				                              'type': type,
		                              })
		
		ElementTree(root).write(self.trafficdataxml)

	def updatetrafficlights(self):
		tree = ElementTree()
		tree.parse(self.trafficsignaldataxml)

		#Get traffic light states
		#format: ['202305458', 'grygrygrygrygrygry'], ['202305472', 'gGGGgGGGGgGGGgGGGG']]}
		trafficlights = self.getTrafficLightValues()
	
		for trafficlight in trafficlights:
			trafficlightID = trafficlight[0]
			linklights = list(trafficlight[1])
			for link_index in range(len(linklights)):
				items = tree.findall('trafficlight')			
				for item in items:
					# Remove letter g from links
					purestr = str(item.attrib['link_index']).replace("g", "")
					link_indexes = purestr.split("!")
					# if the traffic light id matches
					if str(item.attrib['intersection_id']) == trafficlightID:						
						# if link index is in the link_indexes
						if str(link_index) in link_indexes:
							item.attrib['state'] = getTrafficLightState(linklights[link_index])
							#logging.info({"id":trafficlight,"intersection id":trafficlightID,"link index":link_index, "state":getTrafficLightState(linklights[link_index])})
							


		tree.write(self.trafficsignaldataxml)

	def getTrafficLightValues(self):
		lights = self.traciinst.trafficlights.getIDList()
		lightsdata = [[0 for col in range(2)] for row in range(len(lights))]
		lightindex = 0
		for light in lights:
			lightsdata[lightindex][0] = light
			lightsdata[lightindex][1] = self.traciinst.trafficlights.getRedYellowGreenState(light)
			lightindex += 1

		return lightsdata
		
	def getSimTime(self):
		# in seconds
		return (self.traciinst.simulation.getCurrentTime()/1000.0);
		
	def simulationStep(self, additionalSteps=0):
		if additionalSteps == 0:
			# move one step
			self.traciinst.simulationStep(0)
		else:			
			current_sim_time = self.getSimTime();
			logging.info('Simulation: ' + self.strSimulationID + ', time: ' + str(current_sim_time));
			# current_sim_time is in seconds, convert to ms
			target_time = (current_sim_time + additionalSteps*self.step_duration*2)*1000
			self.traciinst.simulationStep(target_time)
	
	def getpos(self):
		vehicles = self.traciinst.vehicle.getIDList()
		vehpos = [[0 for col in range(4)] for row in range(len(vehicles))]
		vehindex = 0
		for veh in vehicles:
			carCoord = self.traciinst.vehicle.getPosition(veh)
			carGeoCoord = self.traciinst.simulation.convertGeo(carCoord[0], carCoord[1])
			vehpos[vehindex][0] = veh
			vehpos[vehindex][1] = carGeoCoord[1]
			vehpos[vehindex][2] = carGeoCoord[0]
			vehpos[vehindex][3] = self.traciinst.vehicle.getTypeID(veh)
			vehindex += 1
			
		return vehpos
	
	def setMaxSpeed(self, laneID, maxSpeed):
		print("Lane ID: %s  Max Speed: %s" % (laneID, maxSpeed))
		self.traciinst.lane.setMaxSpeed(str(laneID), maxSpeed*0.444)
		tree = ElementTree()
		tree.parse(self.trafficdataxml)
		items = tree.findall('speedsigns/speedsign')
		for item in items:
			if str(item.attrib['id']) == "speedsign_0":
				item.set('speedlimit', str(maxSpeed))

		tree.write(self.trafficdataxml)
	
