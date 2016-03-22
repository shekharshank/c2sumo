from SimulationDAO import *
from constants import *
from DAO import VehiclesDAO
from util import *
import json
import gviz_api
		
class SimulationExecutionInterface (object):

	def __init__(self, sim_exec_id, sim_id):
		self.id = sim_exec_id
		self.sim_id = sim_id
		
	def getSimulationExecutionDetails(self):
		return getSimulationExecutionDetails(self.id)

	def getSimulationResult(self):
			
  		
		description = {"count": ("string", "Number of Vehicles"),
		               "waittime": ("number", "Avg. Wait Time in secs"),
		               "type": ("string", "Vehicle Type"),
		               "speed": ("string", "Avg. Speed in mph")}
		               
		
		# get data
		loop_data = getSimulationResultByVehicleType(self.id)
		
		# get all vehicle types
		sim_id = self.sim_id
		vehicle_dao = VehiclesDAO()
		vehicle_types = vehicle_dao.readVehicles(str(sim_id))
		vehicle_dict = {}
		for vehicle in vehicle_types:
			vehicle_dict[str(vehicle["_id"])] = vehicle["name"]
		
		data = []
		total_vehicles = 0
		weighted_speed = 0
		total_waittime = 0
		
		#loop through result set  and aggregate data to add sum for all vehicle types
		
		for row in loop_data['result']:
			vehicle_data = {}
			# should not happen, only for experiments
			if vehicle_dict.get(row["_id"]) is None:
				vehicle_data["type"] = row["_id"]
			else:
				vehicle_data["type"] = vehicle_dict[row["_id"]]
						
			vehicle_data["speed"] = round(row["speed"], 2)
			# calculate average wait time from total
			vehicle_data["waittime"] = round(row["waittime"]*1.0/row["count"], 2)
			vehicle_data["count"] = row["count"]
			
			total_vehicles += vehicle_data["count"]
			weighted_speed += vehicle_data["speed"]*vehicle_data["count"]
			total_waittime += row["waittime"]
			data.append(vehicle_data)
			
		# add the total row
		if total_vehicles != 0:
			vehicle_data = {}
			vehicle_data["type"] = "All"
			vehicle_data["speed"] = round(weighted_speed/total_vehicles, 2)
			vehicle_data["waittime"] = round(total_waittime/total_vehicles, 2)
			vehicle_data["count"] = total_vehicles
			data.append(vehicle_data)
		
		data_table = gviz_api.DataTable(description)
		data_table.LoadData(data)
		return_data = json.loads(data_table.ToJSon(columns_order=("type", "speed", "waittime", "count"),
                                order_by="type"))
  		return return_data
  		
  	def getInductionLoopResult(self, loop_id, flow_rate_agg):
  	  		
  		duration = getSimulationDuration(self.id)
		return_data = duration
  		all_vehicle_types = getDistinctSimulatedVehicleList(self.id, loop_id)
  		description = {"endtime": ("number", "Time Step")}
  		
  			
  		# no data is present, retur 
  		if len(all_vehicle_types) == 0:
  			description["vehicletype"] = ("number", "Default")
  			data_table = gviz_api.DataTable(description)
  			return_data['chart_data'] = data_table.ToJSon()
  			return return_data
  			 						               
	 	loop_data = getSimulationInducionLoopResult(self.id, loop_id)
		 	
	 	aggregation_interval = int(flow_rate_agg)
	 	present_step = aggregation_interval
	 	data = []
	 	vehicle_type_dict = {}
	 	col_order = ("endtime",)
		for vehicle_type in all_vehicle_types:
				description[vehicle_type] = ("number", vehicle_type)
				col_order += (vehicle_type,)
		description['All'] = ("number", 'All')
		col_order += ('All',)
		
	 	for row in loop_data:
		 	
		 	# will be 10 initially	
	 		if row["endtime"] <= present_step:
	 			if vehicle_type_dict.get(row["vehicletype"]) is None:
	 				vehicle_type_dict[row["vehicletype"]] = row["count"]	 				
	 			else:
	 				vehicle_type_dict[row["vehicletype"]] += row["count"]
	 			# Add entry for all vehicles	
	 			if vehicle_type_dict.get('All') is None:
	 				vehicle_type_dict['All'] = row["count"]	 				
	 			else:
	 				vehicle_type_dict['All'] += row["count"]	 				
		 	else:
	 			# add an entry
	 			aggregate_row = {}
	 			# this should be 11, 21 etc.
				aggregate_row["endtime"] = present_step
				for vehicle in all_vehicle_types:
					if vehicle_type_dict.get(vehicle) is None:
						aggregate_row[vehicle] = 0
					else:
						aggregate_row[vehicle] = vehicle_type_dict[vehicle]
					
					aggregate_row['All'] = vehicle_type_dict.get('All')
						
				data.append(aggregate_row)
				
				# reset the dictionary 
				vehicle_type_dict = {}
				present_step += aggregation_interval
				
	 			while (row["endtime"] > present_step):
	 				aggregate_row = {}
	 				aggregate_row["endtime"] = present_step
	 				for vehicle in all_vehicle_types:
						aggregate_row[vehicle] = 0
	 				data.append(aggregate_row)
	 				present_step += aggregation_interval
	 				
	 				aggregate_row['All'] = 0	
	 			
	 			if vehicle_type_dict.get(row["vehicletype"]) is None:
					vehicle_type_dict[row["vehicletype"]] = row["count"]
				else:
	 				vehicle_type_dict[row["vehicletype"]] += row["count"]
	 				
	 			# Add entry for all vehicles	
	 			if vehicle_type_dict.get('All') is None:
	 				vehicle_type_dict['All'] = row["count"]	 				
	 			else:
	 				vehicle_type_dict['All'] += row["count"]	 				
	 			
	 	# add the last interval
	 	aggregate_row = {}
		aggregate_row["endtime"] = row["endtime"]
		if len(vehicle_type_dict) is not 0:
			for vehicle in all_vehicle_types:
				if vehicle_type_dict.get(vehicle) is None:
					aggregate_row[vehicle] = 0
				else:
					aggregate_row[vehicle] = vehicle_type_dict[vehicle]
					
			if vehicle_type_dict.get('All') is None:
				aggregate_row['All'] = 0
			else:
				aggregate_row['All'] = vehicle_type_dict['All']
					
		
		data.append(aggregate_row)	  			 		
						            		
		data_table = gviz_api.DataTable(description)
		
		data_table.LoadData(data)				
		
		chart_data = data_table.ToJSon(columns_order=col_order,
	                            order_by="endtime")
	        		
		return_data["chart_data"] = chart_data	                        
  		return return_data
  			
  	def getQueueResult(self, loop_id):
					
		  		
			description = {"_id": ("number", "Time Step"),
				       "queueinglength": ("number", "Queue Length")}
				               
		 	data = getSimulationQueueResult(self.id, loop_id)
		 	
		 	duration = getSimulationDuration(self.id)
		 	
			data_table = gviz_api.DataTable(description)
			data_table.LoadData(data['result'])
			
			return_data = duration
			chart_data = data_table.ToJSon(columns_order=("_id", "queueinglength"),
		                               order_by="_id")
			return_data["chart_data"] = chart_data
  			return return_data
  		
  	def getInductionDataTable(self, loop_id):
					
		  		
			description = {"property": ("string", "Property"),
				       "value": ("number", "Value")}
			
			
			idparts = loop_id.split("!")		
			location_field = idparts[0]
			junction_id = idparts[1]
			
			
			data = []
		 	flow_data = getSimulationInductionFlowRate(self.id, location_field, junction_id)
		 	# should be single row
		 	for flow_row in flow_data['result']:
		 		row = {}
		 		row["property"] = "Number of Vehicles"
				row["value"] =  flow_row['vehiclecount']
		 		data.append(row)
		 		row = {}
		 		row["property"] = "Flow Rate (Vehicles/s)"
		 		row["value"] =  round(flow_row['vehiclecount']*1.0/flow_row['endtime'],3)
		 		data.append(row)		 		
		 	
		 		if idparts[0][-2:] == "in":
		 		
		 			# get lanes adjascent to the loop_id
					adjascent_routes_data = getAdjascentLanes(location_field, junction_id)
					lane = adjascent_routes_data[location_field]
					adjascent_route_field = location_field + "_adjascent"
					adjascent_routes = adjascent_routes_data[adjascent_route_field]
					left_lane = adjascent_routes[0]
					straight_lane = adjascent_routes[1]
					right_lane = adjascent_routes[2]
										
					all_routes_data = getSimulatedVehicleRoutes(self.id)
					
					left = 0
					straight = 0
					right = 0
					
					for route_row in all_routes_data:
						route = route_row['route']
							
						length = len(route)
						pos = 0
						# only need to check till second last
						while pos < length - 1:
							# found the lane for the vehicle
							if route[pos] == lane:
								# check next matches any adjascent lanes
								if (route[pos + 1] == left_lane):
									left += 1
								elif (route[pos + 1] == straight_lane):
									straight += 1
								elif (route[pos + 1] == right_lane):	
									right += 1
							
							pos += 1
								
					
		 			row = {}
					row["property"] = "Vehicles Turning Left"
					row["value"] =  left
		 			data.append(row)
		 			
		 			row = {}
					row["property"] = "Vehicles Moving Straight"
					row["value"] =  straight
		 			data.append(row)
		 			
		 			row = {}
					row["property"] = "Vehicles Turning Right"
					row["value"] =  right
		 			data.append(row)
		 		
		 			queue_data = getSimulationInductionQueueResult(self.id, location_field, junction_id)
		 			# should be single row
		 			for queue_row in queue_data['result']:
		 				row = {}
		 				row["property"] = "Average Queue Length in ft"
						row["value"] =  round(queue_row['queueinglength'], 2)
		 				data.append(row)
		 				row = {}
		 				row["property"] = "Max Queue Duration in secs"
						row["value"] =  queue_row['maxqueueduration']
		 				data.append(row)
		 				row = {}
						row["property"] = "Total Queue Duration in secs"
						row["value"] =  queue_row['count']
		 				data.append(row)
		 				row = {}
						row["property"] = "Total Queue Length in ft"
						row["value"] =  round(queue_row['totalqueueinglength'], 2)
						#print queue_row['totalqueueinglength']
						#print queue_row['endtime']
		 				data.append(row)
		 				
			data_table = gviz_api.DataTable(description)
			data_table.LoadData(data)
			return_data = json.loads(data_table.ToJSon(columns_order=("property", "value"),
		                               order_by="property"))
  			return return_data
  		
  	

	
	
