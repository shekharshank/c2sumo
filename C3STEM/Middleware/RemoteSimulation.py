import paramiko
import time
import logging
from constants import SSH_LOG_FILE, SUCCESSFUL_CONNECTION_TOKEN, FAILED_CONNECTION_TOKEN, KEY_FILE_PREFIX, KEY_FILE_SUFFIX

#ip_addr = assignVM();
#ip_addr = '10.2.204.56';

paramiko.util.log_to_file(SSH_LOG_FILE)

class RemoteSimulation:
	
	
		def __init__(self, host, keyName, simulationID):
			self.client = paramiko.SSHClient();
			self.client.load_system_host_keys()
			self.simulationID = simulationID
			self.keyName = KEY_FILE_PREFIX + keyName + KEY_FILE_SUFFIX
			self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy());
			try:
				logging.info('Connecting to host: ' + host + ' for starting simulation...');
		        	self.client.connect(host, username='root', key_filename=self.keyName);
		
			except Exception,e:
				try:
					logging.warning('Unable to connect to host: ' + host + ' . Wait for 5 seconds..');
					logging.exception(e)
					time.sleep(5);
					logging.info('Connecting to host: ' + host + ' for starting simulation...');
		        		self.client.connect(host, username='root', key_filename=self.keyName);
				except Exception, e:
					logging.error('Unable to connect to host: ' + host);
					logging.exception(e)
					print 'unable to connect'
			
		# create connection
		def runRemoteSimulation(self):
			
		
			logging.info('Executing start simulation script for id:' + str(self.simulationID));
			command = 'python /opt/C3STEM/Middleware/sumoexec.py ' + str(self.simulationID)
			stdin, stdout, stderr = self.client.exec_command(command);
			print 'Running simulation..'
			
			for line in stdout:
				line = line.strip()
				logging.info(line)
				if(line == SUCCESSFUL_CONNECTION_TOKEN):
					logging.info('Sumo startup was successful')
					return True
				elif (line == FAILED_CONNECTION_TOKEN):
					logging.warning('Connection Failed')
					return False
				 
			logging.error('Unknown Token Received')
			return False
					
			#for line in stderr:
			#	print line
			
		# create connection
		def aggregateSimulationData(self, sim_exec_id):			
		
			logging.info('Executing aggregate simulation data script for id:' + str(sim_exec_id));
			command = 'python /opt/C3STEM/Middleware/aggregatesimdata.py ' + str(self.simulationID) + ' ' + str(sim_exec_id)
			stdin, stdout, stderr = self.client.exec_command(command);
			print 'Aggregating  simulation data..'
			
		def close(self):
			self.client.close();
