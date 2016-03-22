from pymongo import MongoClient
import ConfigParser
import os.path

class DBUtil(object):

	_instance = None
	_initialized = False
	
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
        		cls._instance = super(DBUtil, cls).__new__(cls, *args, **kwargs)
		return cls._instance
        	
	def __init__(self):
		if(self._initialized):
			return
		print "initializing"
		conf = os.path.join(os.path.dirname(__file__), './config/application.conf')
		Config = ConfigParser.ConfigParser();
		Config.read(conf);
		
		connection = MongoClient(Config.get('DB', 'ip'))
		self.db = connection.c3stem_database
		self._initialized = True
        	
        def getDatabase(self):
        	return self.db

