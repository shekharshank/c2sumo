#from cloud.NovaCloudAccess import NovaCloudAccess
from dto.VirtualMachine import VirtualMachine
from dao.CloudDAO import *
import ConfigParser
import os.path
import logging
import time

class CloudInterface:

	def createVM(self):
		cloud = NovaCloudAccess();
		conf = os.path.join(os.path.dirname(__file__), '../config/ec2cloud.conf')
		Config = ConfigParser.ConfigParser();
		Config.read(conf);
		key_name = Config.get('InstanceDetails', 'keyname');
		image =  Config.get('InstanceDetails', 'image');
		flavor =  Config.get('InstanceDetails', 'flavor');
		instance = cloud.createServer(key_name, image, flavor);
		# sleep for 5 seconds so that the IP gets updated
		time.sleep(5);
		details =cloud.getServerDetails(instance.id);
		if details.addresses.get('private') is  None:
			print "retrying fetch...";
			time.sleep(5);
			details =cloud.getServerDetails(instance.id);
			if details.addresses.get('private') is  None:
				print "retrying fetch...";
				time.sleep(5);
				details =cloud.getServerDetails(instance.id);
		vm = VirtualMachine()
		vm.name = details.name;
		vm.id = details.id;
		vm.image = image;
		vm.flavor = flavor;
		vm.key_name = key_name;
		vm.type = "TRANSIENT";
		vm.user = "SPECIFIC";
		vm.host_id = details.hostId;
		vm.public_IP = None;
		vm.private_IP = None;
		if details.addresses.get('private') is not None:
			for addr in details.addresses['private']:
				vm.private_IP = addr['addr'];
		if details.addresses.get('public') is not None:
			for addr in details.addresses['public']:
				vm.public_IP = addr['addr'];
		createVMRecord(vm)
		return vm.name
		

	def deleteVM(self, vm_name):
		cloud = NovaCloudAccess();
		cloud.deleteServer(vm_name);
		return deleteVMRecord(vm_name);

	def assignVM(self, simulation_id):
		# check if VM is already assigned 
		vm_row = getAssignedVMDetailsForSimID(simulation_id)
		if vm_row is not None:
			# VM already assigned, return it
			logging.info('VM: ' + vm_row['private_IP'] + ' found for sim id: ' + str(simulation_id))
			return vm_row	
		
		# need to assign VM		
		logging.info('No VM found for sim id: ' + str(simulation_id) + ' assigning now...');
		return assignVMForSimulationID(simulation_id);
		
	def getAssignedVMDetails(self, simulation_id):
		return getAssignedVMDetailsForSimID(simulation_id)
		
	def freeVMForSimulation(self, simulation_id):
		freeVMForSimID(simulation_id)
