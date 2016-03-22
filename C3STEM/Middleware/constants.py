"""
@file    constants.py
"""
# IP number to communicate with Sumo Server
SUMO_IP = r'"127.0.0.1"'

# Port number to communicate with Sumo Server
SUMO_PORT = 9999

# IP number to connect to database
DB_IP = r'"localhost"'

# Port number to connect to database
DB_PORT = 27017

# Sumo binary file location
SUMO_EXE_LOC = r'"sumo"'

# Sumo config file location
#SUMO_CONFIG_LOC = r'"/opt/C3STEM/Map/westend.sumo.cfg"'
SUMO_CONFIG_LOC = r'"/opt/C3STEM/Map/c2sumo.sumo.cfg"'

#SUMO_CONFIG_LOC_A = r'"/opt/C3STEM/Map/westend.sumo_A.cfg"'
#SUMO_CONFIG_LOC_B = r'"/opt/C3STEM/Map/westend.sumo_B.cfg"'

SUMO_CONFIG_LOC_A = r'"/opt/C3STEM/Map/c2sumo_A.sumo.cfg"'
SUMO_CONFIG_LOC_B = r'"/opt/C3STEM/Map/c2sumo_B.sumo.cfg"'
#SUMO_CONFIG_LOC_C = r'"/opt/C3STEM/Map/c2sumo_C.sumo.cfg"'
#SUMO_CONFIG_LOC_D = r'"/opt/C3STEM/Map/c2sumo_D.sumo.cfg"'

# Sumo route file location
SUMO_NET_LOC_SUFFIX = r'".net.xml"'
#SUMO_NET_LOC = r'"/opt/C3STEM/Map/c2sumo_chattanooga_map.net.xml"'
#SUMO_NET_LOC_A = r'"/opt/C3STEM/Map/c2sumo_chattanooga_map_A.net.xml"'
#SUMO_NET_LOC_B = r'"/opt/C3STEM/Map/c2sumo_chattanooga_map_B.net.xml"'
#SUMO_NET_LOC_C = r'"/opt/C3STEM/Map/c2sumo_chattanooga_map_C.net.xml"'
#SUMO_NET_LOC_D = r'"/opt/C3STEM/Map/c2sumo_chattanooga_map_D.net.xml"'

# Sumo route file location
#SUMO_ROUTES_LOC = r'"/opt/C3STEM/Map/westend.rou.xml"'
SUMO_ROUTES_LOC = r'"/opt/C3STEM/Map/c2sumo.rou.xml"'

# Sumo is started automatically
SUMO_AUTO_START = "true"

GENERATED_DATA_FOLDER = r'/opt/C3STEM/Middleware/static/Data/'

# Location of TrafficData.xml
TRAFFIC_DATA_XML = r'/TrafficData.xml'

# Location of TrafficSignalData.xml
TRAFFIC_SIGNAL_DATA_XML = r'/TrafficSignalData.xml'

STOP_SIGN_DATA_XML = r'/StopSignData.xml'

# Sumo turn file location
#SUMO_TURN_LOC = r'/opt/C3STEM/Map/westend.turn.xml'
SUMO_TURN_LOC = r'/opt/C3STEM/Map/c2sumo.turn.xml'

# Sumo closed lanes file location
SUMO_CLOSED_LANES_LOC = r'/opt/C3STEM/Map/westend_closed_lanes.turn.xml'

# Sumo closed lanes file location for north south bound
SUMO_NS_CLOSED_LANES_LOC = r'/opt/C3STEM/Map/westend_ns_closed_lanes.turn.xml'

# Sumo flow file location
#SUMO_FLOW_LOC = r'/opt/C3STEM/Map/westend.flow.xml'
SUMO_FLOW_LOC = r'/opt/C3STEM/Map/c2sumo.flow.xml'

# Sumo vehicle type location
#SUMO_VEHICLE_TYPE_LOC = r'/opt/C3STEM/Map/westend.vtype.xml'
SUMO_VEHICLE_TYPE_LOC = r'/opt/C3STEM/Map/c2sumo.vtype.xml'

# Sumo traffic light logic file location
#SUMO_TRAFFIC_LIGHT_LOC = r'/opt/C3STEM/Map/westend.tlLogic.xml'
SUMO_TRAFFIC_LIGHT_LOC = r'/opt/C3STEM/Map/c2sumo.tlLogic.xml'

SUMO_INDUCTION_LOOP_LOC = r'/opt/C3STEM/Map/westend.induction.xml' 

# End simulation time for jtrrouter
ROUTE_SIM_END = 300

# JTR binary file location
JTRROUTER_EXE_LOC = r'"jtrrouter"'

# Simulation sleep period in ms for run function
SIM_SLEEP_PERIOD = 0.25

#induction output file
INDUCTION_OUT_FILE = r'/opt/C3STEM/Middleware/static/Data/induction_out.xml'

#queue output file
QUEUE_OUT_FILE = r'/opt/C3STEM/Middleware/static/Data/queue_out.xml'

#trip output file
TRIP_OUT_FILE = r'/opt/C3STEM/Middleware/static/Data/tripinfo_out.xml'

#route output file
ROUTE_OUT_FILE = r'/opt/C3STEM/Middleware/static/Data/routeinfo_out.xml'

#server log file
SERVER_LOG_FILE = r'/opt/C3STEM/Middleware/log/server.log'

#simulation log file
SIMULATION_LOG_FILE = r'/opt/C3STEM/Middleware/log/simulation.log'

#paramiko log file
SSH_LOG_FILE = r'/opt/C3STEM/Middleware/log/ssh.log'

#sumo message log file
SUMO_MESSAGE_LOG_FILE = r'/opt/C3STEM/Middleware/log/sumo.log'

#sumo error log file
SUMO_ERROR_LOG_FILE = r'/opt/C3STEM/Middleware/log/sumo_error.log'

#sumo log level
LOG_LEVEL = r'INFO'

# induction loop aggregation interval - providing as input now
#AGGREGATION_INTERVAL = 50

# message for communication
SUCCESSFUL_CONNECTION_TOKEN = r'SUCCESSFUL_CONNECTION_TOKEN'
FAILED_CONNECTION_TOKEN = r'FAILED_CONNECTION_TOKEN'

KEY_FILE_PREFIX= r'/opt/C3STEM/Middleware/'
KEY_FILE_SUFFIX= r'.pem'
