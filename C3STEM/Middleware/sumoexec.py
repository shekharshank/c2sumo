from SimulationEngine import SimulationEngine
from constants import SIMULATION_LOG_FILE, LOG_LEVEL
from bson.objectid import ObjectId
import sys
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s -line %(lineno)d - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename= SIMULATION_LOG_FILE, level=LOG_LEVEL)
logging.info('#############################################################################');


simulationID = sys.argv[1]  #"52d70ffd80109358763c9c59"
logging.info('Starting simulation for simulation id: ' + simulationID);
try:
	sim = SimulationEngine(ObjectId(simulationID))
	sim.configureSimulator();
except Exception, err:
	logging.exception(err)
