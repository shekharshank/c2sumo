#!python
import sys
import time
import os.path
import xml.etree.ElementTree as xml
from threading import Thread
import xml.etree.ElementTree
from xml.etree.ElementTree import ElementTree, Element, SubElement, dump
from xml.etree.ElementTree import tostring
from xml.dom import minidom
sys.path.append('/opt/C3STEM/Middleware/')
from auth import AuthController, require, has_role, has_privileges
from calculatestats import *
import json
import logging
import copy
import cherrypy
import _strptime

from bson.objectid import ObjectId
from bson import Binary, Code
from bson.json_util import dumps
from bson.json_util import loads
from datetime import datetime

from constants import *
#from EventQueue import *
from DAO import *
from SimulationExecutionInterface import *
from SimulationBackupInterface import *
from cloud.CloudInterface import *
from UserDataInterface import *
from RemoteSimulation import *
from c3stem import trafficsim
from SimulationEngine import *

#e_queue = EventQueue()
SESSION_AUTH_KEY = '_cp_auth'

class Root:
	
	#_cp_config = {
        #	'tools.sessions.on': True,
        #	'tools.auth.on': True
	#}
	
	auth = AuthController()
	vehDAO = VehiclesDAO()
	turnProbabilityDAO = TurnProbabilityDAO()
	trafficLightDAO = TrafficLightDAO()
	flowsDAO = FlowsDAO()
	studentGroupDAO = StudentGroupDAO()
	problemsDAO = ProblemsDAO()
	reasonDAO = ReasonDAO()
	simAssociationDAO = SimAssociationDAO()
	colabLogDAO = ColabLogDAO()
	
	def __init__(self, ospath):			
		self.ospath = ospath
	
	@cherrypy.expose
	def prettify(self):
		#Use this method to test an internal function"""
		#rough_string = tostring(elem, 'utf-8')
		#reparsed = minidom.parseString(rough_string)
		#return reparsed.toprettyxml(indent="  ")
		SimulationEngine("53e4ef02d527600fa63a69f6").createFlowsXml();
		
	@cherrypy.expose
	@require()
	def index(self):
		#return open( self.ospath + '/static/TrafficSim.html')
		raise cherrypy.HTTPRedirect("/static/TrafficSim.html")
	
	@cherrypy.expose
	@require(has_role("admin"))
	def updateStepRate(self, steps):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
					simualtionID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format
					interface = UserDataInterface()
					interface.updateSimulationStepRate(simualtionID, steps)
					return {'response' : {'status': "success"}}
		else:
			return {'response' :  {'status':"failure"}}
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	#@require(has_role("admin"))	
	def setMaxSpeed(self, laneID, maxSpeed):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simualtionID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format
			#cherrypy.t_sim = trafficsim(e_queue, simualtionID)		
			#e_queue.enqueue(cherrypy.t_sim.setMaxSpeed, [str(laneID), float(maxSpeed)], {})
			return {'response' : {'status': "success", 'speed' : maxSpeed}}
		else:
			return {'response' :  {'status':"failure"}}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def getGroupData(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			interface = UserDataInterface()
			username = cherrypy.request.login
			simulationID = interface.getSimulationid(username) #pass in this format			
			siminfo = interface.getSimulationUserData(simulationID)
			simFolderId = interface.getSimulationFolder(simulationID, username)
			problem_id = self.problemsDAO.getProblemID(simulationID)
			problem = self.problemsDAO.readProblem(problem_id)
			interface.ensureSimulationExists(simFolderId)
			sim_folder = str(simFolderId)
			userDetails = interface.getUserDetails(username)
			firstname = userDetails.get('firstname')
			lastname = userDetails.get('lastname')
			groupname = userDetails.get('groupname')
			
			# In COLAB mode, rest of the junctions are returned in addition to the
			# one that a group is associated with			
			assoc_junctions = interface.getJunctionsofOtherGroups(simulationID, username)
			logging.info('Returning login data for user: ' + username)
			response_data = {'response' : {'status': "success", 'siminfo' : siminfo, 'sim_id' : str(simulationID),
				'sim_folder' : sim_folder, 'username': cherrypy.request.login, 'firstname':firstname,
				'lastname':lastname, 'groupname':groupname, 'assoc_junctions':assoc_junctions,
				'problem_id' : problem_id, 'problem_type' : problem['type']}}
			logging.info(response_data)
			return response_data
		else:
			return {'response' :  {'status':"failure"}}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	def getAllGroupNames(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			groupNames = self.studentGroupDAO.readAllGroupNames()
			return {'response' : {'status': "success", 'groups': groupNames["name"]}}
		else:
			return {'response' :  {'status':"failure"}}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	def updateGroupUrl(self, group_id, user_id, problem_id, collaboration_url):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			self.studentGroupDAO.updateCollaborationUrl(group_id, collaboration_url)
			
			simulationID = UserDataInterface().getSimulationid(user_id)
			self.colabLogDAO.createLog(simulationID, user_id, problem_id)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  {'status':"failure"}}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	def colabHistory(self):
		users = ['user103','user104','user105','user106','user110','user111','user118','user123','user124','user125','user126','user127','user131','user132']
		cell_list = []
		for usr in users:
			colab_logs = self.colabLogDAO.readCollaboration(usr)
			start_colab_time = datetime(2000, 10, 21, 0, 0)
			next_colab_time = datetime(2000, 10, 21, 0, 0)
			previous_colab_time = datetime(2000, 10, 21, 0, 0)
			for lg in colab_logs:
				next_colab_time = datetime.strptime(lg["colab_time"], "%Y-%m-%d %H:%M:%S.%f")
				diff_colab_time = (next_colab_time-previous_colab_time).total_seconds()
				if diff_colab_time > 160325397:
					start_colab_time = next_colab_time
				elif diff_colab_time > 10:
					end_colab_time = previous_colab_time
					duration = (end_colab_time-start_colab_time).total_seconds()
					cell = {"started on":str(start_colab_time), "ended on":str(end_colab_time), "user_id": str(lg["user_id"]), "simulation_id": str(lg["simulation_id"]),"problem_id": str(lg["problem_id"]), "duration":duration}
					cell_list.append(cell)
					start_colab_time = next_colab_time
				
				previous_colab_time = next_colab_time
		return cell_list
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getGroupUrl(self, group_id):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			interface = UserDataInterface()
			simualtionID = interface.getSimulationid(cherrypy.request.login) #pass in this format			
			groupinfo = interface.getSimulationUserData(simualtionID) #groupinfo["group_id"]
			studentgroup = self.studentGroupDAO.getCollaborationUrl(group_id)
			actual_time = datetime.now()
			last_update_time = datetime.strptime(studentgroup["last_update_time"], "%Y-%m-%d %H:%M:%S.%f") 
			diff = (actual_time - last_update_time).total_seconds()
			#if has_role_of("admin"):
			if diff > 10: 
				collaboration_url = studentgroup["collaboration_url_admin"] + "&group_name=" + group_id
			else:
				collaboration_url = studentgroup["collaboration_url"] + "?group_name=" + group_id				
			return {'response' : {'status': "success", 'collaboration_url' : collaboration_url}}
		else:
			return {'response' :  {'status':"failure"}}		
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getGroupStatusAll(self, rows=None, sidx=None, _search=None, searchField=None, searchOper=None, searchString=None, page=None, sord=None, nd=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			studentgroups = self.studentGroupDAO.readAllGroupNames()
			actual_time = datetime.now()
			cell_list = []
			for grp in studentgroups:
				last_update_time = datetime.strptime(grp["last_update_time"], "%Y-%m-%d %H:%M:%S.%f"); 
				diff = (actual_time - last_update_time).total_seconds()
				if diff > 10:
					collaboration_url = grp["collaboration_url_admin"] + "&group_name=" + str(grp["_id"])
					cell = {"cell": [str(grp["_id"]), "offline", grp["name"], collaboration_url]}
					cell_list.append(cell)
				else:
					collaboration_url = grp["collaboration_url"] + "?gid=000000000000&group_name=" + str(grp["_id"])
					cell = {"cell": [str(grp["_id"]), "online", grp["name"], collaboration_url]}
					cell_list.append(cell)					

			return {'groups' : cell_list}
		else:
			return {'response' :  {'status':"failure"}}			
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getVehicleResult(self, sim_id):
			interface = UserDataInterface()			
			currentSimulationID = interface.getSimulationid(cherrypy.request.login)
			if str(currentSimulationID) == sim_id:
				# just a hack for now
				sim_id = ObjectId(interface.getSimulationFolder(ObjectId(sim_id), (cherrypy.request.login)))
			sim_exec_id = interface.getActiveSimulationExecId(sim_id)
			if(sim_exec_id is None):
				return {'response' :  {'status':"nosimexec"}}
			simInterface = SimulationExecutionInterface(sim_exec_id, sim_id)
			execResult = simInterface.getSimulationExecutionDetails()
			if execResult.get('simendtime') is not None:
				result = simInterface.getSimulationResult()
				return {'response' : {'status': "success", 'resultdata' : result, 'steps':execResult['steps'] }}
			else:
				return {'response' :  {'status':"notloaded"}}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getInductinLoopResult(self, sim_id, loop_id, flow_rate_agg):
			interface = UserDataInterface()
			currentSimulationID = interface.getSimulationid(cherrypy.request.login)
			if str(currentSimulationID) == sim_id:
				# just a hack for now
				sim_id = ObjectId(interface.getSimulationFolder(ObjectId(sim_id), (cherrypy.request.login)))
			sim_exec_id = interface.getActiveSimulationExecId(sim_id)
			if(sim_exec_id is None):
				return {'response' :  {'status':"nosimexec"}}
			simInterface = SimulationExecutionInterface(sim_exec_id, sim_id)
			execResult = simInterface.getSimulationExecutionDetails()
			if execResult.get('simendtime') is not None:
				result = simInterface.getInductionLoopResult(loop_id, flow_rate_agg)
				return {'response' : {'status': "success", 'resultdata' : result}}
			else:
				return {'response' :  {'status':"notloaded"}}			
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getInductionDataTable(self, sim_id, loop_id):
			interface = UserDataInterface()			
			currentSimulationID = interface.getSimulationid(cherrypy.request.login)
			if str(currentSimulationID) == sim_id:
				# just a hack for now
				sim_id = ObjectId(interface.getSimulationFolder(ObjectId(sim_id), (cherrypy.request.login)))
			sim_exec_id = interface.getActiveSimulationExecId(sim_id)
			if(sim_exec_id is None):
				return {'response' :  {'status':"nosimexec"}}	
			simInterface = SimulationExecutionInterface(sim_exec_id, sim_id)
			execResult = simInterface.getSimulationExecutionDetails()
			if execResult.get('simendtime') is not None:
				result = simInterface.getInductionDataTable(loop_id)
				return {'response' : {'status': "success", 'resultdata' : result}}
			else:
				return {'response' :  {'status':"notloaded"}}
			
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def getQueueResult(self, sim_id, loop_id):
		interface = UserDataInterface()			
		currentSimulationID = interface.getSimulationid(cherrypy.request.login)
		if str(currentSimulationID) == sim_id:
			# just a hack for now
			sim_id = ObjectId(interface.getSimulationFolder(ObjectId(sim_id), (cherrypy.request.login)))
		sim_exec_id = interface.getActiveSimulationExecId(sim_id)
		if(sim_exec_id is None):
			return {'response' :  {'status':"nosimexec"}}	
		simInterface = SimulationExecutionInterface(sim_exec_id, sim_id)
		execResult = simInterface.getSimulationExecutionDetails()
		if execResult.get('simendtime') is not None:
			result = simInterface.getQueueResult(loop_id)
			return {'response' : {'status': "success", 'resultdata' : result}}
		else:
			return {'response' :  {'status':"notloaded"}}			

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def startSimulation(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true" and has_privileges():
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format
			logging.info('Starting simulation for user: ' +  cherrypy.request.login + ' with Simulation id: ' + str(simulationID))
			cloudInterface = CloudInterface();
			logging.info('Fetching assigned VM for simulation id: ' + str(simulationID))
			vm = cloudInterface.assignVM(simulationID);
			if vm is None:
				logging.warning('VM could not assigned for user: ' +  cherrypy.request.login + ' with Simulation id: ' + str(simulationID))
				return {'response' : 'notavailable'}
			private_ip = vm['private_IP'];
			key_name = vm['key_name'];
			logging.info('Starting simulation for id: ' + str(simulationID) + ' on VM:' + private_ip)
			trafficsim(private_ip, key_name, simulationID, cherrypy.request.login).start()
			#t_sim.start()
			logging.info('Simulation start command sent..');
			return {'response' : str(simulationID)}
		else:
			logging.warning('Unable to authenticate..')
			return {'response' :  "failure"}
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def checkStartStatus(self):		
		interface = UserDataInterface()
		simulationID = interface.getSimulationid(cherrypy.request.login)
		return {'response' : interface.getSimulationRunningState(simulationID)}
		

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def speedupSimulation(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true" and has_privileges():
			interface = UserDataInterface()
			simulationID = interface.getSimulationid(cherrypy.request.login) #pass in this format
			logging.info('Speeding simulation for user: ' +  cherrypy.request.login + ' with Simulation id: ' + str(simulationID))			
			interface.speedUpSimulationStepRate(simulationID);
			return {'response' : "success"}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def finishSimulation(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true" and has_privileges():
			interface = UserDataInterface()
			simulationID = interface.getSimulationid(cherrypy.request.login) #pass in this format
			logging.info('Finishing simulation for user: ' +  cherrypy.request.login + ' with Simulation id: ' + str(simulationID))			
			interface.finishSimulation(simulationID);
			return {'response' : "success"}
		else:
			return {'response' :  "failure"}	
			
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def restoreSimulationRate(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true" and has_privileges():
			interface = UserDataInterface()
			simulationID = interface.getSimulationid(cherrypy.request.login) #pass in this format
			logging.info('Restoring simulation rate for user: ' +  cherrypy.request.login + ' with Simulation id: ' + str(simulationID))			
			interface.restoreSimulationStepRate(simulationID);
			return {'response' : "success"}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def setMode(self, user_mode=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			username = cherrypy.request.login
			interface = UserDataInterface()
			simulationID = interface.getSimulationid(username)
			logging.info('simulationID: ' + str(simulationID) + ' , username: ' + username + ', usermode: ' + user_mode)
			interface.setUserMode(username, user_mode);
			cloudInterface = CloudInterface();			
			simFolderId = interface.getSimulationFolder(simulationID, username)
			interface.ensureSimulationExists(simFolderId)
			return {'response' : {'status': "success", 'sim_folder' : str(simFolderId),
				 'sim_id' : str(simulationID)}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def readVehicles(self, rows=None, sidx=None, _search=None, searchField=None, searchOper=None, searchString=None, page=None, sord=None, nd=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format
			vehicles = self.vehDAO.readVehicles(simulationID)			
			cell_list = []
			for veh in vehicles:
				cell = {"id":str(veh["_id"]), "cell": [str(veh["_id"]), str(veh["simulation_id"]), veh["name"], veh["accel"], veh["decel"], veh["sigma"], veh["max_speed"], veh["length"], veh["probability"]]}
				cell_list.append(cell)
			#vehiclesJson ={"vehicles": cell_list}
			return {"vehicles": cell_list}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def readSimulations(self, rows=None, sidx=None, _search=None, searchField=None, searchOper=None, searchString=None, page=None, sord=None, nd=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			result = UserDataInterface().getSimulationHistoryTable(cherrypy.request.login)
			return {"sim_versions": result}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def saveSimVersion(self, sim_id, sim_label, shared_with):		
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			# no need to get sim id from user
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			interface = SimulationBackupInterface(simulationID)
			return interface.saveSimulationWithUserInput(sim_label, shared_with, cherrypy.request.login);
		else:
			return {'response' :  "failure"}
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def copySimVersionToCurrent(self, sim_id):		
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			currentSimulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			interface = SimulationBackupInterface(currentSimulationID)
			interface.copySimulationToCurrent(sim_id, cherrypy.request.login);
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def deleteSimVersion(self, id, oper=None ):		
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			# accepts object id format
			interface = SimulationBackupInterface(ObjectId(id))
			interface.deleteSimulation(cherrypy.request.login);
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def deleteVehicle(self, id, oper=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":		
			self.vehDAO.deleteVehicle(id)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))		
	def updateVehicle(self, id, accel, decel, sigma, max_speed, length, probability, oper=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":			
			self.vehDAO.updateVehicle(id, accel, decel, sigma, max_speed, length, probability)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def createVehicle(self, name, accel, decel, sigma, max_speed, length, probability, oper=None, id=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format			
			self.vehDAO.createVehicle(simulationID, name, accel, decel, sigma, max_speed, length, probability)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def readAllFlows(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			user_id = cherrypy.request.login
			interface = UserDataInterface()
			simulationID = interface.getSimulationid(cherrypy.request.login)
			problem_id = self.problemsDAO.getProblemID(simulationID)
			user_mode = interface.getUserMode(user_id)
			
			# Find duplicates and discard them from the list
			# At the collaboration mode first discard duplicated flow points
			# then flow points with removable column equals to 1 from the remaining list
			
			'''
			temp_flows = copy.deepcopy(flows)
			duplicates = {}
			for flow in temp_flows:
				if flow["point_name"] in duplicates.keys():
					duplicates[flow["point_name"]] += 1
				else:
					duplicates[flow["point_name"]] = 1
			'''

			cell_list = []
			if (problem_id in ['3', '8']):
				pointList = ['A', 'B', 'Iwest', 'Lnorth']
			elif (problem_id in ['6a', '6b', '6c']):
				pointList = ['A', 'Iwest']
			else:
				pointList = ['A', 'B', 'C', 'D', 'Jnorth', 'Lnorth']
			
			simIDs = self.simAssociationDAO.readAssociatedSimIDs(simulationID)
			for simID in simIDs:
				flows = self.flowsDAO.readAllFlows(simID)
				for flow in flows:				
					cell = {"id": str(flow["_id"]), "simulation_id": str(flow["simulation_id"]), "point_name": flow["point_name"], "intersection_id": flow["intersection_id"], "latitude": flow["latitude"], "longitude": flow["longitude"], "flow_rate": flow["flow_rate"]}
					#cell = {"point_name": flow["point_name"]}
					if user_mode == "COLAB":
						# If there are duplicates in COLAB mode remove those points
						#if duplicates[flow["point_name"]] == 1 and flow["removable"] == "0":
						if any(flow["point_name"] in s for s in pointList):
							cell_list.append(cell)
					else:
						if any(flow["point_name"] in s for s in pointList):
							if flow["removable"] == "0":
								cell_list.append(cell)

			return {'response' : {'status': "success", 'flows' : cell_list}}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def readFlow(self, point_name):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			flow = self.flowsDAO.readFlow(simulationID, point_name)
			return {'response' : {'status': "success", 'flow_rate' : str(flow["flow_rate"])}}
		else:
			return {'response' :  "failure"}
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def updateFlow(self, point_name, flow_rate):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			self.flowsDAO.updateFlow(simulationID, point_name, flow_rate)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}
			
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def readTurnProbability(self, edge_id):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			turn = self.turnProbabilityDAO.readTurnProbability(simulationID, edge_id)
			return {'response' : {'status': "success", 'left_turn' : turn["left_turn"], 'right_turn' : turn["right_turn"], 'go_straight' : turn["go_straight"]}}
		else:
			return {'response' :  "failure"}
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()
	def updateTurnProbability(self, edge_id, left_turn, right_turn, go_straight):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			self.turnProbabilityDAO.updateTurnProbability(simulationID, edge_id, left_turn, right_turn, go_straight)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def readTrafficLightLogic(self, intersection_id, light_index, rows=None, sidx=None, _search=None, searchField=None, searchOper=None, searchString=None, page=None, sord=None, nd=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			traffic_light_logic = self.trafficLightDAO.readTrafficLightLogic(simulationID, intersection_id)
			# Split phase info and return. Format is in Green!0!1-Red!2!3!4-Green!5-... 
			cell_list = []			
			for tll in traffic_light_logic:
				links = tll["state"].split("-")
				state = ""
				cell = []
				cell.append(str(tll["intersection_id"]))
				for link in links:
					state = link.split("!")
					cell.append(state[0])
				
				cell.append(tll["duration"])
				#cell = {"id":str(tll["_id"]), "cell": [str(tll["intersection_id"]), states, tll["duration"]]}
				cell_indv = {"id":str(tll["_id"]), "cell": cell}
				cell_list.append(cell_indv)

			logicJson ={"logic": cell_list}
			return logicJson
		else:
			return {'response' :  "failure"}


	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))		
	def createTrafficLightLogic(self, intersection_id, state0, state1, state2, state3, state4, state5, state6, state7, traffic_light_links, duration, oper=None, id=None):	
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":	
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			# Put phases into Green!0!1-Red!2!3!4-Green!5-... format
			# traffic_light_links : 0!1-2!3!4-5!6
			indexes = traffic_light_links.split("-")
			state = state0 + "!" + indexes[0] + "-" + state1 + "!" + indexes[1] + "-" + state2 + "!" + indexes[2] + "-" + state3 + "!" + indexes[3] + "-" + state4 + "!" + indexes[4] + "-" + state5 + "!" + indexes[5] + "-" + state6 + "!" + indexes[6] + "-" + state7 + "!" + indexes[7]
			self.trafficLightDAO.createTrafficLightLogic(simulationID, intersection_id, state, duration)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def updateTrafficLightLogic(self, id, duration, oper=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			self.trafficLightDAO.updateTrafficLightLogic(id, duration)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}
			
	'''
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def updateTrafficLightLogic(self, id, state0, state1, state2, state3, state4, state5, state6, state7, traffic_light_links, duration, oper=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			logging.info("updateTrafficLightLogic" + traffic_light_links);
			indexes = traffic_light_links.split("-")
			state = state0 + "!" + indexes[0] + "-" + state1 + "!" + indexes[1] + "-" + state2 + "!" + indexes[2] + "-" + state3 + "!" + indexes[3] + "-" + state4 + "!" + indexes[4] + "-" + state5 + "!" + indexes[5] + "-" + state6 + "!" + indexes[6] + "-" + state7 + "!" + indexes[7]		
			self.trafficLightDAO.updateTrafficLightLogic(id, state, duration)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}
	'''
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))	
	def deleteTrafficLightLogic(self, id, oper=None):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			self.trafficLightDAO.deleteTrafficLightLogic(id)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def readProblem(self, problem_id):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			problem = self.problemsDAO.readProblem(problem_id)
			return {'response' : {'status': "success", 'title' : problem["title"], 'description' : problem["description"]}}
		else:
			return {'response' :  "failure"}

	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def getProblemID(self):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login)
			problem_id = self.problemsDAO.getProblemID(simulationID)
			return {'response' : {'status': "success", 'problem_id' : problem_id}}
		else:
			return {'response' :  "failure"}
					
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require(has_role("admin"))
	def updateProblem(self, problem_id):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":
			username = cherrypy.request.login
			simulationID = UserDataInterface().getSimulationid(username)			
			interface = UserDataInterface()	
			usermode = interface.getUserMode(username)
			if (usermode == 'COLAB' and (not interface.checkIfMasterSim(simulationID))):
				return {'response' :  "not_admin"}
			self.problemsDAO.updateProblem(simulationID, problem_id)
			problem = self.problemsDAO.readProblem(problem_id)
			return {'response' : {'status': "success", 'title' : problem["title"], 'problem_type' : problem["type"]}}
		else:
			return {'response' :  "failure"}			
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@require()	
	def createReason(self, desc):
		if cherrypy.session.get(SESSION_AUTH_KEY, None) is "true":			
			simulationID = UserDataInterface().getSimulationid(cherrypy.request.login) #pass in this format			
			user_id = cherrypy.request.login
			problem_id = self.problemsDAO.getProblemID(simulationID) 
			self.reasonDAO.createReason(simulationID, user_id, problem_id, desc)
			return {'response' : {'status': "success"}}
		else:
			return {'response' :  "failure"}
			
#Start Web Server
ospathvar = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s -line %(lineno)d - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename= SERVER_LOG_FILE, level=LOG_LEVEL)
logging.info('#############################################################################');	
logging.getLogger('cherrypy.access').propagate = False
logging.getLogger('paramiko').propagate = False

conf = os.path.join(os.path.dirname(__file__), 'server.conf')
application = cherrypy.Application(Root(ospathvar), script_name=None, config=conf)

logging.info('Started C3STEM server...');

