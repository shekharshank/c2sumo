import  subprocess, sys
import logging


from constants import *
from calculatestats import aggregateSimData
from DAO import *
from util import *
from UserDataInterface import *
from SimulationBackupInterface import *

class SimulationEngine:

	vehDAO = VehiclesDAO()
	turnProbabilityDAO = TurnProbabilityDAO()
	trafficLightDAO = TrafficLightDAO()
	simulationDAO = SimulationDAO()
	simAssociationDAO = SimAssociationDAO()
	flowsDAO = FlowsDAO()	
        	
	def __init__(self, simulationID):
		logging.info('Initializing simulation for ID: ' + str(simulationID))
		self.simulationID = simulationID
       
		self.sumoexeloc = SUMO_EXE_LOC
		self.sumoport = SUMO_PORT
		self.sumoautostart = SUMO_AUTO_START
		self.queueoutfile = QUEUE_OUT_FILE
		self.tripoutfile = TRIP_OUT_FILE
		self.routeoutfile = ROUTE_OUT_FILE
		self.sumomessagelog = SUMO_MESSAGE_LOG_FILE
		self.sumoerrorlog = SUMO_ERROR_LOG_FILE
		#self.simsleepperiod = float(SIM_SLEEP_PERIOD)
		self.sumoRouteLoc = SUMO_ROUTES_LOC
		self.turnDocLoc = SUMO_TURN_LOC
		self.closedLanesLoc = SUMO_CLOSED_LANES_LOC		
		self.flowDocLoc = SUMO_FLOW_LOC
		self.routeSimEnd = ROUTE_SIM_END
		self.jtrrouterexeLoc = JTRROUTER_EXE_LOC
		self.trafficLightLoc = SUMO_TRAFFIC_LIGHT_LOC
		self.vehicleTypeLoc = SUMO_VEHICLE_TYPE_LOC
		self.inductionLoopLoc = SUMO_INDUCTION_LOOP_LOC
		
		interface = UserDataInterface()
		simdata = interface.getSimulationUserData(self.simulationID)
		print("initializing...")
		if simdata is None:
			print("Not able to initialize...")
			logging.error("Not able to initialize simulation: " + str(self.simulationID))

		group_type = interface.getGroupType(simdata['group_id'])
		logging.info(str(simdata))
		print str(simdata)
		
		problem_id = simdata['problem_id']
		problem = ProblemsDAO().readProblem(problem_id)
		self.sumoNetLoc = problem['map_loc_prefix']
		self.problemType = problem['type']
		
		if (problem_id in ['6a', '6b', '6c']):
			self.closedLanesLoc = SUMO_NS_CLOSED_LANES_LOC
		
		if group_type == 'A':
			self.sumoconfloc = SUMO_CONFIG_LOC_A
			self.sumoNetLoc = self.sumoNetLoc + '_A' 
		elif group_type == 'B':
			self.sumoconfloc = SUMO_CONFIG_LOC_B
			self.sumoNetLoc = self.sumoNetLoc + '_B' 
		#elif group_type == 'C':
		#	self.sumoconfloc = SUMO_CONFIG_LOC_C
		#	self.sumoNetLoc = SUMO_NET_LOC_C
		#elif group_type == 'D':
		#	self.sumoconfloc = SUMO_CONFIG_LOC_D
		#	self.sumoNetLoc = SUMO_NET_LOC_D
		else:
			self.sumoconfloc = SUMO_CONFIG_LOC			
		
		self.sumoNetLoc = self.sumoNetLoc + SUMO_NET_LOC_SUFFIX
	
		
	def connectSumo(self):
		""" Runs sumo simulation as a server
				and opens TraCI connection 
		"""
		print("connecting...")
		sumologfile = open(self.sumomessagelog, 'a')
		sumoerrlogfile = open(self.sumoerrorlog, 'a')
		sumocommand1 = "%s -c %s -n %s -v true  --remote-port %s  --step-length 0.50" % (self.sumoexeloc, self.sumoconfloc, self.sumoNetLoc, self.sumoport)
		sumocommand2 =	" -S %s --queue-output %s --tripinfo-output %s --vehroute-output %s --time-to-teleport 50 --lanechange.allow-swap true" % (self.sumoautostart,
				self.queueoutfile, self.tripoutfile, self.routeoutfile)
		sumocommand = sumocommand1 + sumocommand2
		logging.info('Problem type: ' + self.problemType)
		if (self.problemType == "TRAFFIC_SIGNAL"):
			logging.info('Adding traffic light data to sim config ...')
			sumocommand = sumocommand + " -a " + self.trafficLightLoc + "," + self.vehicleTypeLoc + "," + self.inductionLoopLoc
		else:
			sumocommand = sumocommand + " -a " + self.vehicleTypeLoc + "," + self.inductionLoopLoc
		self.sumoProcess = subprocess.Popen(sumocommand, shell=True, stdout=sumologfile, stderr=sumoerrlogfile)
		
		print("%s -c %s --remote-port %s -S %s" % (self.sumoexeloc, self.sumoconfloc, self.sumoport, self.sumoautostart))		
		
	def configureSimulator(self):
		try:
			print("running...")
			logging.info('Performing simulation steps for id: ' + str(self.simulationID));
			#self.connectDB()
			logging.info('Generating simulation files: ');
			self.createAllFiles()
			logging.info('Starting sumo server..');
			self.connectSumo()
			logging.info('Sumo Server started..');
			print (SUCCESSFUL_CONNECTION_TOKEN)
			
		except Exception, err:
			logging.exception(err)
			print (FAILED_CONNECTION_TOKEN)
			
	
	def aggregateData(self, simExecID):
		try:
			print("aggregating data...")
			logging.info('Aggregating data for simExecID: ' + str(simExecID));
			aggregateSimData(simExecID)
			
		except Exception, err:
			logging.exception(err)			
	
	
	def trafficLightState(self, state):
		""" Returns SUMO 
		traffic light state char
		"""
		if state == "Green":
			return "G"
		elif state ==  "Yellow":
			return "y"
		elif state == "Red":
			return "r"

	def generatePhase(self, state):
		""" Generates phases in the 
		format that SUMO requires
		Format:
			Green!0!1-Red!2!3!4-Green!5-... 		
		"""
		sumoState = ""
		lights = state.split("-")
		for light in lights:
			parts = light.split("!")
			for i in range(len(parts)-1):
				index = parts[i+1]
				# if index has g char in it force it to be green light
				# right turn lights are always green.				
				if index.find("g") == -1:
					sumoState = sumoState + self.trafficLightState(parts[0])
				else:
					sumoState = sumoState + "g"
					
		return sumoState
	
	def createAllFiles(self):
		""" Creates all the necessary files
		that SUMO needs to run the simulation
		"""
		
		# delete old files
		try:
			# causing issues with sim results, will see later
			#os.remove(self.queueoutfile)
			#os.remove(self.tripoutfile)
			#os.remove(self.routeoutfile)
				
			os.remove(self.turnDocLoc)
			os.remove(self.vehicleTypeLoc)		
			os.remove(self.trafficLightLoc)
			os.remove(self.flowDocLoc)		
			os.remove(self.sumoRouteLoc)
		except OSError:
    			pass
		
		
		self.createTurnProbabilityXml()
		self.createVehiclesXml()
		if (self.problemType == "TRAFFIC_SIGNAL"):
			self.createTrafficLightLogicFile()
		self.createFlowsXml()
		self.createRouteFile()
	
	def createTrafficLightLogicFile(self):
		""" Create traffic light logic file as an additional file
		Reads trafficlightlogic table and generates the SUMO phases 
		"""
		# Add traffic logic additional file
		# http://sumo.sourceforge.net/xsd/additional_file.xsd
		tlFile = open(self.trafficLightLoc, 'w')
		tlFile.write("<additional xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"http://sumo.sf.net/xsd/additional_file.xsd\">\n")
				
		simIDs = list(self.simAssociationDAO.readAssociatedSimIDs(self.simulationID))
		
		for simID in simIDs:
			intersectionIDs = self.simulationDAO.getIntersectionIDs(simID)
			for intID in intersectionIDs:
				#print {"simID":simID, "intID":intID}
				trafficLogic = list(self.trafficLightDAO.readAllTrafficLightLogic(simID, intID))		
				# Generate the traffic light phase
				# Format:
				# Green!0!1-Red!2!3!4-Green!5-... 								
				tlFile.write("<tlLogic id=\"")
				tlFile.write(str(intID))
				tlFile.write("\" ") 
				tlFile.write("type=\"static\" offset=\"0\">\n")				
				for tll in trafficLogic:					
					# Generate phase(s)
					if (str(tll["duration"]) != "0"):
						phase = self.generatePhase(tll["state"])
						tlFile.write("<phase ")
						tlFile.write("duration=\"")
						tlFile.write(str(tll["duration"]))
						tlFile.write("\" ")
						tlFile.write("state=\"")
						tlFile.write(str(phase))
						tlFile.write("\"/")
						tlFile.write(">\n")				
													
				tlFile.write("</tlLogic>\n")
			
		
		tlFile.write("</additional>\n")

	def createRouteFile(self):
		""" Create route file by executing jtrrouter application
		which requires several parameters.
		logging.info('Generating Route File...')
		"""
		
		
		sumologfile = open(self.sumomessagelog, 'a')
		sumoerrlogfile = open(self.sumoerrorlog, 'a')
		# No closed lane
		#sumocommand1 = "%s --flow-files=%s --turn-ratio-files='%s,%s' --random=true" % (self.jtrrouterexeLoc, self.flowDocLoc, self.turnDocLoc, self.closedLanesLoc)
		sumocommand1 = "%s --flow-files=%s --turn-ratio-files='%s,%s' --random=true" % (self.jtrrouterexeLoc, self.flowDocLoc, self.turnDocLoc, self.closedLanesLoc)
		sumocommand2 =	" --net-file=%s --output-file=%s --ignore-errors=true --begin 0 --end %s --max-edges-factor 0.0002" % (self.sumoNetLoc, self.sumoRouteLoc, self.routeSimEnd)
		sumocommand = sumocommand1 + sumocommand2	
		self.jtrProcess = subprocess.Popen(sumocommand, shell=True, stdout=sumologfile, stderr=sumoerrlogfile)		
		self.jtrProcess.wait()
		logging.info('Route File Generated')

	def createVehiclesXml(self):
		""" Create Vehicle distribution and types and save them in vType
		file for each id in simulationID list.
		""" 
		# Add vehicle type additional file
		# http://sumo.sourceforge.net/xsd/additional_file.xsd
		vehFile = open(self.vehicleTypeLoc, 'w')

		#vehFile.write("<additional xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"http://sumo.sf.net/xsd/additional_file.xsd\">\n")
		vehFile.write("<vTypeDistribution id=\"vehicledist1\">\n")

		#total_veh = 0
		
		#for simID in self.simulationIDs:
		#	vehicles = self.vehDAO.readVehicles(simID)
		#	for veh in vehicles:
		#		total_veh = total_veh + int(veh["vehicle_count"])				
		
		simIDs = self.simAssociationDAO.readAssociatedSimIDs(self.simulationID)
		#veh_index = 0
		for simID in simIDs:
			# Connect db and pull up the vehicle information
			sim_record = UserDataInterface().getSimulationUserData(simID)
			vehicles = self.vehDAO.readVehicles(simID)
			for veh in vehicles:
				veh_id = veh["name"] + "_" +  str(sim_record.get('group_id'))
				#veh_index = veh_index + 1
				#probability = int(veh["vehicle_count"]) * 1.0 / total_veh
				
				vehFile.write("<vType ")
				vehFile.write("id=\"")
				vehFile.write(str(veh_id))
				vehFile.write("\" ")				
				vehFile.write("accel=\"")
				vehFile.write(str(convertFeetToMeter(float(veh["accel"]))))
				vehFile.write("\" ")
				vehFile.write("decel=\"")
				vehFile.write(str(convertFeetToMeter(float(veh["decel"]))))
				vehFile.write("\" ")
				vehFile.write("length=\"")
				vehFile.write(str(convertFeetToMeter(float(veh["length"]))))
				vehFile.write("\" ")
				vehFile.write("maxSpeed=\"")
				vehFile.write(str(convertMPHToMperSec(float(veh["max_speed"]))))
				vehFile.write("\" ")
				vehFile.write("sigma=\"")
				vehFile.write(str(veh["sigma"]))
				vehFile.write("\" ")
				vehFile.write("probability=\"")
				vehFile.write(str(veh["probability"]))
				vehFile.write("\">")

				# TODO: Need to design a new car following model or just have the default params here
				#vehFile.write("<carFollowing-Krauss accel=\"5.0\" decel=\"5.0\" sigma=\"0.50\"/>\n")
				vehFile.write("</vType>\n")
		
		
		vehFile.write("</vTypeDistribution>\n")
		#vehFile.write("</additional>\n")
		return True
	
	def createTurnProbabilityXml(self):
		""" Retrieves turn probabilities for each junctions and create 
		turn.xml file for SUMO
		"""
		#closedLaneFile = open(self.closedLanesLoc, 'r')
		#closedLanes = closedLaneFile.read()
		
		flowFile = open(self.turnDocLoc, 'w')

		flowFile.write("<turn-defs>\n")
		flowFile.write("<interval begin=\"0\" end=\"300\">\n")

		simIDs = list(self.simAssociationDAO.readAssociatedSimIDs(self.simulationID))
				
		# Connect db and pull up the probabilities 
		for simID in simIDs:
			intersectionIDs = self.simulationDAO.getIntersectionIDs(simID)
			for intID in intersectionIDs:
				turnProbabilities = self.turnProbabilityDAO.readAllTurnProbabilities(simID, intID)
				for prob in turnProbabilities:
					flowFile.write("<fromEdge id=\"")
					flowFile.write(str(prob["edge_id"]))
					flowFile.write("\">\n")				
					flowFile.write("<toEdge id=\"")
					flowFile.write(str(prob["to_edge_left"]))
					flowFile.write("\" probability=\"")
					flowFile.write(str(prob["left_turn"]))
					flowFile.write("\"/>\n")			
					flowFile.write("<toEdge id=\"")
					flowFile.write(str(prob["to_edge_right"]))
					flowFile.write("\" probability=\"")
					flowFile.write(str(prob["right_turn"]))
					flowFile.write("\"/>\n")				
					flowFile.write("<toEdge id=\"")
					flowFile.write(str(prob["to_edge_straight"]))
					flowFile.write("\" probability=\"")
					flowFile.write(str(prob["go_straight"]))
					flowFile.write("\"/>\n")				
					flowFile.write("</fromEdge>\n")
		
				
		#flowFile.write(closedLanes)
		flowFile.write("</interval>\n")
		flowFile.write("</turn-defs>\n")	
	
	def createFlowsXml(self):
		""" Retrieves flows for a simulation and 
		creates flow.xml file for SUMO
		"""		
		flowFile = open(self.flowDocLoc, 'w')
		
		simIDs = list(self.simAssociationDAO.readAssociatedSimIDs(self.simulationID))
		#simID = simIDs[0]		
			
		flowFile.write("<flows>\n")

		# Connect db and pull up the flows 
		#	intersectionIDs = self.simulationDAO.getIntersectionIDs(simID)
		#	for intID in intersectionIDs:		
		interface = UserDataInterface()
		simdata = interface.getSimulationUserData(self.simulationID)
		user_mode = simdata['mode']
		problem_id = simdata['problem_id']
		
		if (problem_id in ['3', '8']):
			pointList = ['A', 'B', 'Iwest', 'Lnorth']
		elif (problem_id in ['6a', '6b', '6c']):
			pointList = ['A', 'Iwest']
		else:
			pointList = ['A', 'B', 'C', 'D', 'Jnorth', 'Lnorth']
		flows = []

		simIDs = self.simAssociationDAO.readAssociatedSimIDs(self.simulationID)
		for simID in simIDs:
			temp_flows = self.flowsDAO.readAllFlows(simID)
			for flow in temp_flows:				
				if user_mode == "COLAB":
					if any(flow["point_name"] in s for s in pointList):
						flows.append(flow)
				else:
					if any(flow["point_name"] in s for s in pointList):
						if flow["removable"] == "0":
							flows.append(flow)
		'''					
		temp_flows = []
		for simID in simIDs:
			logging.info('Simulation ID:' + str(simID))
			temp = list(self.flowsDAO.readAllFlows(simID))
			temp_flows.extend(temp)
				
		flows = []
		duplicates = {}
		for flow in temp_flows:
			logging.info('Flow Point Name: ' + flow["point_name"])
			if flow["point_name"] in duplicates.keys():
				duplicates[flow["point_name"]] += 1
			else:
				duplicates[flow["point_name"]] = 1

			# If there are duplicates in COLAB mode remove those points
			# If point appended list only one time and if it is not a removable
			# point then add it to the flows list
			if duplicates[flow["point_name"]] == 1 and flow["removable"] == "0":
				flows.append(flow)
		'''
		
		for flow in flows:			
			flowFile.write("<flow id=\"flow_")
			flowFile.write(str(flow["from_edge_id"]))
			flowFile.write("\" from=\"")
			flowFile.write(str(flow["from_edge_id"]))
			flowFile.write("\" via=\"")
			flowFile.write(str(flow["via_edge_id"]))
			flowFile.write("\" ")
			flowFile.write(" begin=\"0\" end=\"300\"")
			flowFile.write(" vehsPerHour=\"")
			flowFile.write(str(flow["flow_rate"]))
			flowFile.write("\" type=\"vehicledist1\"")
			flowFile.write("/>\n")
				
		flowFile.write("</flows>\n")	
	
