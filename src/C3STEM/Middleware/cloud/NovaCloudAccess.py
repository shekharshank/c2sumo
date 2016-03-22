from novaclient.v1_1 import client
import os.path
import ConfigParser
from random import randint

class NovaCloudAccess(object) :

	_initialized = False

	def __init__(self):
		if(self._initialized):
			return;
		conf = os.path.join(os.path.dirname(__file__), '../config/ec2cloud.conf')
		Config = ConfigParser.ConfigParser();
		Config.read(conf);
		self.nova = client.Client(Config.get('AuthDetails', 'username'), Config.get('AuthDetails', 'password'), Config.get('AuthDetails', 'tenant'), Config.get('AuthDetails', 'authURL'), service_type="compute");
		self._initialized = True;

	def getServerList(self):
		servers = self.nova.servers.list();
		for server in servers:
			print server.name, server.id
		#	print server.get_console_output()


	def getImageList(self):
		images = self.nova.images.list();
		for image in images:
			print image.name, image.id
	
	
	def getFlavorList(self):
		flavors = self.nova.flavors.list();
		for flavor in flavors:
			print flavor.name, flavor.id

	
	def createServer(self, key_name, image_name, flavor_name):
		server_name = "c3stem_" + str(randint(1,100000000))
		image = self.nova.images.find(name=image_name)
		flavor = self.nova.flavors.find(name=flavor_name)
		instance =self.nova.servers.create(name=server_name, image=image, flavor=flavor, key_name=key_name);
		return instance

	def getServerDetails(self, server_id):
		return self.nova.servers.get(server_id);
	

	def deleteServer(self, server_name):
		server = self.nova.servers.find(name=server_name)
		self.nova.servers.delete(server.id);
