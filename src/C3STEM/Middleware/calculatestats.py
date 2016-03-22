from pymongo import MongoClient
from xml.etree.ElementTree import ElementTree
from constants import *
from util import *
from DBUtil import *

def aggregateSimData(sim_exec_id):
	persistInductionData(sim_exec_id)
	persistQueueingData(sim_exec_id)
	persistTripData(sim_exec_id)

def aggregateInduction(sim_exec_id, simulation_id):

	tree = ElementTree()
	tree.parse(INDUCTION_OUT_FILE)
	intervals = tree.findall('interval')
	
	# get all induction loops associated with our simulation
	db = DBUtil().getDatabase()
    	junctions = db.simulation.find_one({"_id":simulation_id})["junctions"]
    	db_loops = []
    	for junction in junctions:
    		loops = db.inductionloop.find({"junction":junction})
    		db_loops.extend(loops)
    		
    	# create blank dict    	
    	# vehicle count at each induction loop of each vehicle type
    	loop_data = {}
    	for interval in intervals:
    		vehiclecount = int(interval.attrib['nVehContrib'])
		if vehiclecount != 0:
			# valid data 
			xmlloop_id = interval.attrib['id']
			
			# check which junction and type it belongs to
			junction = None
			location = None
			for db_loop in db_loops:
				if(db_loop["_id"] == xmlloop_id):
					junction = db_loop["junction"]
					location = db_loop["location"]
					break
					
			loop_dict_id = junction + "+" + location			
			if loop_data.get(loop_dict_id) is None:
				loop_data[loop_dict_id] = vehiclecount
			else:
				loop_data[loop_dict_id] = loop_data[loop_dict_id] + vehiclecount				
	
	print loop_data
			
def persistInductionData(sim_exec_id):

	db = DBUtil().getDatabase()
	
	tree = ElementTree()
	tree.parse(INDUCTION_OUT_FILE)
	intervals = tree.findall('interval')
	    		
    	for interval in intervals:
    		vehiclecount = int(interval.attrib['nVehContrib'])
		if vehiclecount != 0:
			# valid data 
			xmlloop_id = interval.attrib['id']
			intervals_with_type = interval.findall('typedInterval')
			for interval_with_type in intervals_with_type:
				type_vehicle_count = int(interval_with_type.attrib['nVehContrib'])
				if type_vehicle_count != 0:
					db.simulation_induction_data.insert({
					"sim_exec_id": sim_exec_id,
					"induction_id": interval_with_type.attrib['id'],
					"begintime": float(interval_with_type.attrib['begin']),
					"endtime": float(interval_with_type.attrib['end']),
					"vehicletype": interval_with_type.attrib['type'],
					"count": type_vehicle_count					
					})
					
def persistQueueingData(sim_exec_id):

	# work around for invalid xml generate in sumo version 0.17.1
	f = open(QUEUE_OUT_FILE, 'r')
	outfile = open(QUEUE_OUT_FILE + ".cleaned", 'w')
	for line in f:
		linestring = line.rstrip('\n')
		if(linestring == "/>"):
			outfile.write("		</lanes>\n")
		elif(linestring != ">"):
			outfile.write(line)
	outfile.flush();
	f.close()
	outfile.close()

	db = DBUtil().getDatabase()	
	tree = ElementTree()
	tree.parse(QUEUE_OUT_FILE + ".cleaned")
	steps = tree.findall('data')
	    		
    	for step in steps:
    		lanes = step.findall('lanes/lane')
    		if lanes is not None:
    		
    			for lane in lanes:
    				db.simulation_queue_data.insert({
					"sim_exec_id": sim_exec_id,
					"lane_id": lane.attrib['id'],
					"timestep": float(step.attrib['timestep']),
					"queueingtime": float(lane.attrib['queueing_time']),
					 #store in feet instead of meters
					"queueinglength": convertMeterToFeet(float(lane.attrib['queueing_length']))
					})
    		
			
def persistTripData(sim_exec_id):

	db = DBUtil().getDatabase()
	
	tree = ElementTree()
	tree.parse(TRIP_OUT_FILE)
	trips = tree.findall('tripinfo')
	
	route_tree = ElementTree()
	route_tree.parse(ROUTE_OUT_FILE)
	routes = route_tree.findall('vehicle')
	    		
    	for trip in trips:
    		vehicle_route = []
    		for route in routes:
    			if route.attrib['id'] == trip.attrib['id']:
    				edge_string = route.find('route').attrib['edges']
    				vehicle_route = edge_string.split()
    				break
    				
    		db.simulation_trip_data.insert({
					"sim_exec_id": sim_exec_id,
					"vehicle_id": trip.attrib['id'],
					"departstep": float(trip.attrib['depart']),
					"duration": float(trip.attrib['duration']),
					"routelength": convertMeterToFeet(float(trip.attrib['routeLength'])),
					"waittime": float(trip.attrib['waitSteps']),
					"speed": convertMperSecToMPH(float(trip.attrib['routeLength']) / float(trip.attrib['duration'])),
					"type": trip.attrib['vType'],
					"route": vehicle_route
					})