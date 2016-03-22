def convertMperSecToMPH(mps):
	return round(2.23694*mps,2)
	
def convertMeterToFeet(m):
	return round(3.28084*m,2)

def convertMPHToMperSec(mph):
	return round(0.44704*mph,2)
	
def convertFeetToMeter(feet):
	return round(0.3048*feet,2)
	
def getTrafficLightState(s):
	return {
		'r': "Red",
		'y': "Yellow",
		'g': "Green",
		'G': "Green"
	}[s] 