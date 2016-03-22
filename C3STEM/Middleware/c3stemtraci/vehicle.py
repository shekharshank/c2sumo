# -*- coding: utf-8 -*-
"""
@file    vehicle.py
@author  Michael Behrisch
@author  Lena Kalleske
@date    2011-03-09
@version $Id: vehicle.py 15402 2014-01-14 14:07:54Z namdre $

Python implementation of the TraCI interface.

SUMO, Simulation of Urban MObility; see http://sumo-sim.org/
Copyright (C) 2011-2013 DLR (http://www.dlr.de/) and contributors

This file is part of SUMO.
SUMO is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.
"""
import struct
from traciclass import Storage, SubscriptionResults
import constants as tc

DEPART_TRIGGERED = -1
DEPART_NOW = -2

STOP_DEFAULT = 0
STOP_PARKING = 1
STOP_TRIGGERED = 2


def _readBestLanes(self, result):
    result.read("!iB")
    nbLanes = result.read("!i")[0] # Length
    lanes = []
    for i in range(nbLanes):
        result.read("!B")
        laneID = result.readString()
        length, occupation, offset = result.read("!BdBdBb")[1::2]
        allowsContinuation = result.read("!BB")[1]
        nextLanesNo = result.read("!Bi")[1]
        nextLanes = []
        for j in range(nextLanesNo):
            nextLanes.append(result.readString())
        lanes.append( [laneID, length, occupation, offset, allowsContinuation, nextLanes ] )
    return lanes

def _readLeader(self, result):
    result.read("!iB")
    vehicleID = result.readString()
    result.read("!B")
    dist = result.readDouble()
    if vehicleID:
        return vehicleID, dist
    return None

class Vehicle:
    
            def __init__(self,  traciInst):  
                    self.traciInst = traciInst

                    self._RETURN_VALUE_FUNC = {tc.ID_LIST:             Storage.readStringList,
                      tc.ID_COUNT:            Storage.readInt,
                      tc.VAR_SPEED:           Storage.readDouble,
                      tc.VAR_SPEED_WITHOUT_TRACI: Storage.readDouble,
                      tc.VAR_POSITION:        lambda result: result.read("!dd"),
                      tc.VAR_ANGLE:           Storage.readDouble,
                      tc.VAR_ROAD_ID:         Storage.readString,
                      tc.VAR_LANE_ID:         Storage.readString,
                      tc.VAR_LANE_INDEX:      Storage.readInt,
                      tc.VAR_TYPE:            Storage.readString,
                      tc.VAR_ROUTE_ID:        Storage.readString,
                      tc.VAR_COLOR:           lambda result: result.read("!BBBB"),
                      tc.VAR_LANEPOSITION:    Storage.readDouble,
                      tc.VAR_CO2EMISSION:     Storage.readDouble,
                      tc.VAR_COEMISSION:      Storage.readDouble,
                      tc.VAR_HCEMISSION:      Storage.readDouble,
                      tc.VAR_PMXEMISSION:     Storage.readDouble,
                      tc.VAR_NOXEMISSION:     Storage.readDouble,
                      tc.VAR_FUELCONSUMPTION: Storage.readDouble,
                      tc.VAR_NOISEEMISSION:   Storage.readDouble,
                      tc.VAR_EDGE_TRAVELTIME: Storage.readDouble,
                      tc.VAR_EDGE_EFFORT:     Storage.readDouble,
                      tc.VAR_ROUTE_VALID:     lambda result: bool(result.read("!B")[0]),
                      tc.VAR_EDGES:           Storage.readStringList,
                      tc.VAR_SIGNALS:         Storage.readInt,
                      tc.VAR_LENGTH:          Storage.readDouble,
                      tc.VAR_MAXSPEED:        Storage.readDouble,
                      tc.VAR_ALLOWED_SPEED:   Storage.readDouble,
                      tc.VAR_VEHICLECLASS:    Storage.readString,
                      tc.VAR_SPEED_FACTOR:    Storage.readDouble,
                      tc.VAR_SPEED_DEVIATION: Storage.readDouble,
                      tc.VAR_EMISSIONCLASS:   Storage.readString,
                      tc.VAR_WAITING_TIME:    Storage.readDouble,
                      tc.VAR_WIDTH:           Storage.readDouble,
                      tc.VAR_MINGAP:          Storage.readDouble,
                      tc.VAR_SHAPECLASS:      Storage.readString,
                      tc.VAR_ACCEL:           Storage.readDouble,
                      tc.VAR_DECEL:           Storage.readDouble,
                      tc.VAR_IMPERFECTION:    Storage.readDouble,
                      tc.VAR_TAU:             Storage.readDouble,
                      tc.VAR_BEST_LANES:      _readBestLanes,
                      tc.VAR_LEADER:          _readLeader,
                      tc.DISTANCE_REQUEST:    Storage.readDouble,
                      tc.VAR_DISTANCE:        Storage.readDouble}

                    self.subscriptionResults = SubscriptionResults(self._RETURN_VALUE_FUNC)
            
            def _getUniversal(self, varID, vehID):
                result = self.traciInst._sendReadOneStringCmd(tc.CMD_GET_VEHICLE_VARIABLE, varID, vehID)
                return self._RETURN_VALUE_FUNC[varID](result)
            
            def getIDList(self):
                """getIDList() -> list(string)
                
                Returns a list of ids of all vehicles currently running within the scenario.
                """
                return self._getUniversal(tc.ID_LIST, "")
            
            def getIDCount(self):
                """getIDCount() -> integer
                
                Returns the number of vehicle in the network.
                """
                return self._getUniversal(tc.ID_COUNT, "")
            
            def getSpeed(self, vehID):
                """getSpeed(string) -> double
                
                Returns the speed in m/s of the named vehicle within the last step.
                """
                return self._getUniversal(tc.VAR_SPEED, vehID)
            
            def getSpeedWithoutTraCI(self, vehID):
                """getSpeedWithoutTraCI(string) -> double
                
                .
                """
                return self._getUniversal(tc.VAR_SPEED_WITHOUT_TRACI, vehID)
            
            def getPosition(self, vehID):
                """getPosition(string) -> (double, double)
                
                Returns the position of the named vehicle within the last step [m,m].
                """
                return self._getUniversal(tc.VAR_POSITION, vehID)
            
            def getAngle(self, vehID):
                """getAngle(string) -> double
                
                Returns the angle in degrees of the named vehicle within the last step. 
                """
                return self._getUniversal(tc.VAR_ANGLE, vehID)
            
            def getRoadID(self, vehID):
                """getRoadID(string) -> string
                
                Returns the id of the edge the named vehicle was at within the last step.
                """
                return self._getUniversal(tc.VAR_ROAD_ID, vehID)
            
            def getLaneID(self, vehID):
                """getLaneID(string) -> string
                
                Returns the id of the lane the named vehicle was at within the last step.
                """
                return self._getUniversal(tc.VAR_LANE_ID, vehID)
            
            def getLaneIndex(self, vehID):
                """getLaneIndex(string) -> integer
                
                Returns the index of the lane the named vehicle was at within the last step.
                """
                return self._getUniversal(tc.VAR_LANE_INDEX, vehID)
            
            def getTypeID(self, vehID):
                """getTypeID(string) -> string
                
                Returns the id of the type of the named vehicle.
                """
                return self._getUniversal(tc.VAR_TYPE, vehID)
            
            def getRouteID(self, vehID):
                """getRouteID(string) -> string
                
                Returns the id of the route of the named vehicle.
                """
                return self._getUniversal(tc.VAR_ROUTE_ID, vehID)
            
            def getRoute(self, vehID):
                """getRoute(string) -> list(string)
                
                Returns the ids of the edges the vehicle's route is made of.
                """
                return self._getUniversal(tc.VAR_EDGES, vehID)
            
            def getLanePosition(self, vehID):
                """getLanePosition(string) -> double
                
                The position of the vehicle along the lane measured in m.
                """
                return self._getUniversal(tc.VAR_LANEPOSITION, vehID)
            
            def getColor(self, vehID):
                """getColor(string) -> (integer, integer, integer, integer)
                
                Returns the vehicle's rgba color.
                """
                return self._getUniversal(tc.VAR_COLOR, vehID)
            
            def getCO2Emission(self, vehID):
                """getCO2Emission(string) -> double
                
                Returns the CO2 emission in mg for the last time step.
                """
                return self._getUniversal(tc.VAR_CO2EMISSION, vehID)
            
            def getCOEmission(self, vehID):
                """getCOEmission(string) -> double
                
                Returns the CO emission in mg for the last time step.
                """
                return self._getUniversal(tc.VAR_COEMISSION, vehID)
            
            def getHCEmission(self, vehID):
                """getHCEmission(string) -> double
                
                Returns the HC emission in mg for the last time step.
                """
                return self._getUniversal(tc.VAR_HCEMISSION, vehID)
            
            def getPMxEmission(self, vehID):
                """getPMxEmission(string) -> double
                
                Returns the particular matter emission in mg for the last time step.
                """
                return self._getUniversal(tc.VAR_PMXEMISSION, vehID)
            
            def getNOxEmission(self, vehID):
                """getNOxEmission(string) -> double
                
                Returns the NOx emission in mg for the last time step.
                """
                return self._getUniversal(tc.VAR_NOXEMISSION, vehID)
            
            def getFuelConsumption(self, vehID):
                """getFuelConsumption(string) -> double
                
                Returns the fuel consumption in ml for the last time step.
                """
                return self._getUniversal(tc.VAR_FUELCONSUMPTION, vehID)
            
            def getNoiseEmission(self, vehID):
                """getNoiseEmission(string) -> double
                
                Returns the noise emission in db for the last time step.
                """
                return self._getUniversal(tc.VAR_NOISEEMISSION, vehID)
            
            def getPersonNumber(self, vehID):
                """getPersonNumber(string) -> integer
                
                .
                """
                return self._getUniversal(tc.VAR_PERSON_NUMBER, vehID)
            
            def getAdaptedTraveltime(self, vehID, time, edgeID):
                """getAdaptedTraveltime(string, double, string) -> double
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_EDGE_TRAVELTIME, vehID, 1+4+1+4+1+4+len(edgeID))
                self.traciInst._message.string += struct.pack("!BiBiBi", tc.TYPE_COMPOUND, 2, tc.TYPE_INTEGER, time,
                                                     tc.TYPE_STRING, len(edgeID)) + edgeID
                return self.traciInst._checkResult(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_EDGE_TRAVELTIME, vehID).readDouble()
            
            def getEffort(self, vehID, time, edgeID):
                """getEffort(string, double, string) -> double
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_EDGE_EFFORT, vehID, 1+4+1+4+1+4+len(edgeID))
                self.traciInst._message.string += struct.pack("!BiBiBi", tc.TYPE_COMPOUND, 2, tc.TYPE_INTEGER, time,
                                                     tc.TYPE_STRING, len(edgeID)) + edgeID
                return self.traciInst._checkResult(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_EDGE_EFFORT, vehID).readDouble()
            
            def isRouteValid(self, vehID):
                return self._getUniversal(tc.VAR_ROUTE_VALID, vehID)
            
            def getSignals(self, vehID):
                """getSignals(string) -> integer
                
                Returns an integer encoding the state of a vehicle's signals.
                """
                return self._getUniversal(tc.VAR_SIGNALS, vehID)
            
            def getLength(self, vehID):
                """getLength(string) -> double
                
                Returns the length in m of the given vehicle.
                """
                return self._getUniversal(tc.VAR_LENGTH, vehID)
            
            def getMaxSpeed(self, vehID):
                """getMaxSpeed(string) -> double
                
                Returns the maximum speed in m/s of this vehicle.
                """
                return self._getUniversal(tc.VAR_MAXSPEED, vehID)
            
            def getAllowedSpeed(self, vehID):
                """getAllowedSpeed(string) -> double
                
                Returns the maximum allowed speed on the current lane regarding speed factor in m/s for this vehicle.
                """
                return self._getUniversal(tc.VAR_ALLOWED_SPEED, vehID)
            
            def getVehicleClass(self, vehID):
                """getVehicleClass(string) -> string
                
                Returns the vehicle class of this vehicle.
                """
                return self._getUniversal(tc.VAR_VEHICLECLASS, vehID)
            
            def getSpeedFactor(self, vehID):
                """getSpeedFactor(string) -> double
                
                Returns the chosen speed factor for this vehicle.
                """
                return self._getUniversal(tc.VAR_SPEED_FACTOR, vehID)
            
            def getSpeedDeviation(self, vehID):
                """getSpeedDeviation(string) -> double
                
                Returns the maximum speed deviation of the vehicle type.
                """
                return self._getUniversal(tc.VAR_SPEED_DEVIATION, vehID)
            
            def getEmissionClass(self, vehID):
                """getEmissionClass(string) -> string
                
                Returns the emission class of this vehicle.
                """
                return self._getUniversal(tc.VAR_EMISSIONCLASS, vehID)
            
            def getWaitingTime(self, vehID):
                """getWaitingTime() -> double
                The waiting time of a vehicle is defined as the time (in seconds) spent with a
                speed below 0.1m/s since the last time it was faster than 0.1m/s.
                (basically, the waiting time of a vehicle is reset to 0 every time it moves). 
                """
                return self._getUniversal(tc.VAR_WAITING_TIME, vehID)
            
            def getWidth(self, vehID):
                """getWidth(string) -> double
                
                Returns the width in m of this vehicle.
                """
                return self._getUniversal(tc.VAR_WIDTH, vehID)
            
            def getMinGap(self, vehID):
                """getMinGap(string) -> double
                
                Returns the offset (gap to front vehicle if halting) of this vehicle.
                """
                return self._getUniversal(tc.VAR_MINGAP, vehID)
            
            def getShapeClass(self, vehID):
                """getShapeClass(string) -> string
                
                Returns the shape class of this vehicle.
                """
                return self._getUniversal(tc.VAR_SHAPECLASS, vehID)
            
            def getAccel(self, vehID):
                """getAccel(string) -> double
                
                Returns the maximum acceleration possibility in m/s^2 of this vehicle.
                """
                return self._getUniversal(tc.VAR_ACCEL, vehID)
            
            def getDecel(self, vehID):
                """getDecel(string) -> double
                
                Returns the maximum deceleration possibility in m/s^2 of this vehicle.
                """
                return self._getUniversal(tc.VAR_DECEL, vehID)
            
            def getImperfection(self, vehID):
                """getImperfection(string) -> double
                
                .
                """
                return self._getUniversal(tc.VAR_IMPERFECTION, vehID)
            
            def getTau(self, vehID):
                """getTau(string) -> double
                
                Returns the driver's reaction time in s for this vehicle.
                """
                return self._getUniversal(tc.VAR_TAU, vehID)
            
            def getBestLanes(self, vehID):
                """getBestLanes(string) -> 
                
                Information about the wish to use subsequent edges' lanes.
                """
                return self._getUniversal(tc.VAR_BEST_LANES, vehID)
            
            def getLeader(self, vehID, dist=0.):
                """getLeader(string, double) -> (string, double)
                
                Return the leading vehicle id together with the distance.
                The dist parameter defines the maximum lookahead, 0 calculates a lookahead from the brake gap.
                """
                self.traciInst._beginMessage(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_LEADER, vehID, 1+8)
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, dist)
                return _readLeader(self.traciInst._checkResult(tc.CMD_GET_VEHICLE_VARIABLE, tc.VAR_LEADER, vehID))
            
            def subscribeLeader(self, vehID, dist=0., begin=0, end=2**31-1):
                """subscribeLeader(string, double) -> None
                
                Subscribe for the leading vehicle id together with the distance.
                The dist parameter defines the maximum lookahead, 0 calculates a lookahead from the brake gap.
                """
                self.traciInst._subscribe(tc.CMD_SUBSCRIBE_VEHICLE_VARIABLE, begin, end, vehID,
                                 (tc.VAR_LEADER,), {tc.VAR_LEADER: struct.pack("!Bd", tc.TYPE_DOUBLE, dist)})
            
            def getDrivingDistance(self, vehID, edgeID, pos, laneID=0):
                """getDrivingDistance(string, string, double, integer) -> double
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_GET_VEHICLE_VARIABLE, tc.DISTANCE_REQUEST, vehID, 1+4+1+4+len(edgeID) + 8+1+1)
                self.traciInst._message.string += struct.pack("!BiBi", tc.TYPE_COMPOUND, 2,
                                                     tc.POSITION_ROADMAP, len(edgeID)) + edgeID
                self.traciInst._message.string += struct.pack("!dBB", pos, laneID, tc.REQUEST_DRIVINGDIST)
                return self.traciInst._checkResult(tc.CMD_GET_VEHICLE_VARIABLE, tc.DISTANCE_REQUEST, vehID).readDouble()
            
            def getDrivingDistance2D(self, vehID, x, y):
                """getDrivingDistance2D(string, double, double) -> integer
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_GET_VEHICLE_VARIABLE, tc.DISTANCE_REQUEST, vehID, 1+4+1+8+8+1)
                self.traciInst._message.string += struct.pack("!BiBddB", tc.TYPE_COMPOUND, 2,
                                                     tc.POSITION_2D, x, y, tc.REQUEST_DRIVINGDIST)
                return self.traciInst._checkResult(tc.CMD_GET_VEHICLE_VARIABLE, tc.DISTANCE_REQUEST, vehID).readDouble()
            
            def getDistance(self, vehID):
                """getDistance(string) -> double
                
                Returns the distance to the starting point like an odometer
                """
                return self._getUniversal(tc.VAR_DISTANCE, vehID)
            
            def subscribe(self, vehID, varIDs=( tc.VAR_ROAD_ID, tc.VAR_LANEPOSITION), begin=0, end=2**31-1):
                """subscribe(string, list(integer), double, double) -> None
                
                Subscribe to one or more vehicle values for the given interval.
                """
                self.traciInst._subscribe(tc.CMD_SUBSCRIBE_VEHICLE_VARIABLE, begin, end, vehID, varIDs)
            
            def getSubscriptionResults(self, vehID=None):
                """getSubscriptionResults(string) -> dict(integer: <value_type>)
                
                Returns the subscription results for the last time step and the given vehicle.
                If no vehicle id is given, all subscription results are returned in a dict.
                If the vehicle id is unknown or the subscription did for any reason return no data,
                'None' is returned.
                It is not possible to retrieve older subscription results than the ones
                from the last time step.
                """
                return self.subscriptionResults.get(vehID)
            
            def subscribeContext(self, vehID, domain, dist, varIDs=( tc.VAR_ROAD_ID, tc.VAR_LANEPOSITION), begin=0, end=2**31-1):
                self.traciInst._subscribeContext(tc.CMD_SUBSCRIBE_VEHICLE_CONTEXT, begin, end, vehID, domain, dist, varIDs)
            
            def getContextSubscriptionResults(self, vehID=None):
                return self.subscriptionResults.getContext(vehID)
            
            
            def setMaxSpeed(self, vehID, speed):
                """setMaxSpeed(string, double) -> None
                
                Sets the maximum speed in m/s for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_MAXSPEED, vehID, speed)
            
            def setStop(self, vehID, edgeID, pos=1., laneIndex=0, duration=2**31-1, flags=STOP_DEFAULT):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_STOP, vehID, 1+4+1+4+len(edgeID)+1+8+1+1+1+4+1+1)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 5)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(edgeID)) + edgeID
                self.traciInst._message.string += struct.pack("!BdBBBiBB", tc.TYPE_DOUBLE, pos, tc.TYPE_BYTE, laneIndex, tc.TYPE_INTEGER, duration, tc.TYPE_BYTE, flags)
                self.traciInst._sendExact()
            
            def changeLane(self, vehID, laneIndex, duration):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_CHANGELANE, vehID, 1+4+1+1+1+4)
                self.traciInst._message.string += struct.pack("!BiBBBi", tc.TYPE_COMPOUND, 2, tc.TYPE_BYTE, laneIndex, tc.TYPE_INTEGER, duration)
                self.traciInst._sendExact()
            
            def slowDown(self, vehID, speed, duration):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_SLOWDOWN, vehID, 1+4+1+8+1+4)
                self.traciInst._message.string += struct.pack("!BiBdBi", tc.TYPE_COMPOUND, 2, tc.TYPE_DOUBLE, speed, tc.TYPE_INTEGER, duration)
                self.traciInst._sendExact()
            
            def changeTarget(self, vehID, edgeID):
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_CHANGETARGET, vehID, edgeID)
            
            def setType(self, vehID, typeID):
                """setType(string, string) -> None
                
                Sets the id of the type for the named vehicle.
                """
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_TYPE, vehID, typeID)
            
            def setRouteID(self, vehID, routeID):
                """setRouteID(string, string) -> None
                
                Sets the id of the route for the named vehicle.
                """
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_ROUTE_ID, vehID, routeID)
            
            def setRoute(self, vehID, edgeList):
                """
                setRoute(string, list) ->  None
                
                changes the vehicle route to given edges list.
                The first edge in the list has to be the one that the vehicle is at at the moment.
                
                example usage:
                setRoute('1', ['1', '2', '4', '6', '7'])
                
                this changes route for vehicle id 1 to edges 1-2-4-6-7
                """
                if isinstance(edgeList, str):
                    edgeList= [edgeList]
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_ROUTE, vehID,
                                    1+4+sum(map(len, edgeList))+4*len(edgeList))
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRINGLIST, len(edgeList))
                for edge in edgeList:
                    self.traciInst._message.string += struct.pack("!i", len(edge)) + edge
                self.traciInst._sendExact()
            
            def setAdaptedTraveltime(self, vehID, begTime, endTime, edgeID, time):
                """setAdaptedTraveltime(string, double, string, double) -> None
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_EDGE_TRAVELTIME, vehID, 1+4+1+4+1+4+1+4+len(edgeID)+1+8)
                self.traciInst._message.string += struct.pack("!BiBiBiBi", tc.TYPE_COMPOUND, 4, tc.TYPE_INTEGER, begTime,
                                                     tc.TYPE_INTEGER, endTime, tc.TYPE_STRING, len(edgeID)) + edgeID
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, time)
                self.traciInst._sendExact()
            
            def setEffort(self, vehID, begTime, endTime, edgeID, effort):
                """setEffort(string, double, string, double) -> None
                
                .
                """
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_EDGE_EFFORT, vehID, 1+4+1+4+1+4+1+4+len(edgeID)+1+4)
                self.traciInst._message.string += struct.pack("!BiBiBiBi", tc.TYPE_COMPOUND, 4, tc.TYPE_INTEGER, begTime,
                                                     tc.TYPE_INTEGER, endTime, tc.TYPE_STRING, len(edgeID)) + edgeID
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, effort)
                self.traciInst._sendExact()
            
            def rerouteTraveltime(self, vehID, currentTravelTimes = True):
                """rerouteTraveltime(string, bool) -> None
                
                Reroutes a vehicle according to the loaded travel times. If loadTravelTimes is False  
                then the travel times of a loaded weight file or the minimum travel time is used.
                If loadTravelTimes is True (default) then the current traveltime of the edges is loaded and used for rerouting.
                """
                if currentTravelTimes:
                    for edge in self.traciInst.edge.getIDList():
                        self.traciInst.edge.adaptTraveltime(edge, self.traciInst.edge.getTraveltime(edge)) 
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_REROUTE_TRAVELTIME, vehID, 1+4)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 0)
                self.traciInst._sendExact()
            
            def rerouteEffort(self, vehID):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.CMD_REROUTE_EFFORT, vehID, 1+4)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 0)
                self.traciInst._sendExact()
            
            def setSignals(self, vehID, signals):
                """setSignals(string, integer) -> None
                
                Sets an integer encoding the state of the vehicle's signals.
                """
                self.traciInst._sendIntCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_SIGNALS, vehID, signals)
            
            def moveTo(self, vehID, laneID, pos):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_MOVE_TO, vehID, 1+4+1+4+len(laneID)+1+8)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 2)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(laneID)) + laneID
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, pos)
                self.traciInst._sendExact()
            
            def setSpeed(self, vehID, speed):
                """setSpeed(string, double) -> None
                
                Sets the speed in m/s for the named vehicle within the last step.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_SPEED, vehID, speed)
            
            def setColor(self, vehID, color):
                """setColor(string, (integer, integer, integer, integer))
                sets color for vehicle with the given ID.
                i.e. (255,0,0,0) for the color red. 
                The fourth integer (alpha) is currently ignored
                """
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_COLOR, vehID, 1+1+1+1+1)
                self.traciInst._message.string += struct.pack("!BBBBB", tc.TYPE_COLOR, int(color[0]), int(color[1]), int(color[2]), int(color[3]))
                self.traciInst._sendExact()
            
            def setLength(self, vehID, length):
                """setLength(string, double) -> None
                
                Sets the length in m for the given vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_LENGTH, vehID, length)
            
            def setVehicleClass(self, vehID, clazz):
                """setVehicleClass(string, string) -> None
                
                Sets the vehicle class for this vehicle.
                """
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_VEHICLECLASS, vehID, clazz)
            
            def setSpeedFactor(self, vehID, factor):
                """setSpeedFactor(string, double) -> None
                
                .
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_SPEED_FACTOR, vehID, factor)
            
            def setSpeedDeviation(self, vehID, deviation):
                """setSpeedDeviation(string, double) -> None
                
                Sets the maximum speed deviation for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_SPEED_DEVIATION, vehID, deviation)
            
            def setEmissionClass(self, vehID, clazz):
                """setEmissionClass(string, string) -> None
                
                Sets the emission class for this vehicle.
                """
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_EMISSIONCLASS, vehID, clazz)
            
            def setWidth(self, vehID, width):
                """setWidth(string, double) -> None
                
                Sets the width in m for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_WIDTH, vehID, width)
            
            def setMinGap(self, vehID, minGap):
                """setMinGap(string, double) -> None
                
                Sets the offset (gap to front vehicle if halting) for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_MINGAP, vehID, minGap)
            
            def setShapeClass(self, vehID, clazz):
                """setShapeClass(string, string) -> None
                
                Sets the shape class for this vehicle.
                """
                self.traciInst._sendStringCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_SHAPECLASS, vehID, clazz)
            
            def setAccel(self, vehID, accel):
                """setAccel(string, double) -> None
                
                Sets the maximum acceleration in m/s^2 for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_ACCEL, vehID, accel)
            
            def setDecel(self, vehID, decel):
                """setDecel(string, double) -> None
                
                Sets the maximum deceleration in m/s^2 for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_DECEL, vehID, decel)
            
            def setImperfection(self, vehID, imperfection):
                """setImperfection(string, double) -> None
                
                .
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_IMPERFECTION, vehID, imperfection)
            
            def setTau(self, vehID, tau):
                """setTau(string, double) -> None
                
                Sets the driver's reaction time in s for this vehicle.
                """
                self.traciInst._sendDoubleCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_TAU, vehID, tau)
            
            def setLaneChangeMode(self, vehID, lcm):
                """setLaneChangeMode(string, integer) -> None
                
                Sets the vehicle's lane change mode as a bitset.
                """
                self.traciInst._sendIntCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_LANECHANGE_MODE, vehID, lcm)
            
            def add(self, vehID, routeID, depart=DEPART_NOW, pos=0, speed=0, lane=0, typeID="DEFAULT_VEHTYPE"):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.ADD, vehID,
                                    1+4 + 1+4+len(typeID) + 1+4+len(routeID) + 1+4 + 1+8 + 1+8 + 1+1)
                if depart > 0:
                    depart *= 1000
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 6)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(typeID)) + typeID
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(routeID)) + routeID
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_INTEGER, depart)
                self.traciInst._message.string += struct.pack("!BdBd", tc.TYPE_DOUBLE, pos, tc.TYPE_DOUBLE, speed)
                self.traciInst._message.string += struct.pack("!BB", tc.TYPE_BYTE, lane)
                self.traciInst._sendExact()
            
            def addFull(self, vehID, routeID, typeID="DEFAULT_VEHTYPE", depart=None,
                        departLane="0", departPos="base", departSpeed="0",
                        arrivalLane="current", arrivalPos="max", arrivalSpeed="current",
                        fromTaz="", toTaz="", line="", personCapacity=0, personNumber=0):
                messageString = struct.pack("!Bi", tc.TYPE_COMPOUND, 14)
                if depart is None:
                    depart = str(self.traciInst.simulation.getCurrentTime() / 1000.)
                for val in (routeID, typeID, depart, departLane, departPos, departSpeed,
                            arrivalLane, arrivalPos, arrivalSpeed, fromTaz, toTaz, line):
                    messageString += struct.pack("!Bi", tc.TYPE_STRING, len(val)) + val
                messageString += struct.pack("!Bi", tc.TYPE_INTEGER, personCapacity)
                messageString += struct.pack("!Bi", tc.TYPE_INTEGER, personNumber)
            
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.ADD_FULL, vehID, len(messageString))
                self.traciInst._message.string += messageString
                self.traciInst._sendExact()
            
            def remove(self, vehID, reason=tc.REMOVE_VAPORIZED):
                '''Remove vehicle with the given ID for the give reason. 
                   Reasons are defined in module constants and start with REMOVE_'''
                self.traciInst._sendByteCmd(tc.CMD_SET_VEHICLE_VARIABLE, tc.REMOVE, vehID, reason)
            
            def moveToVTD(self, vehID, edgeID, lane, x, y):
                self.traciInst._beginMessage(tc.CMD_SET_VEHICLE_VARIABLE, tc.VAR_MOVE_TO_VTD, vehID, 1+4+1+4+len(edgeID)+1+4+1+8+1+8)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 4)
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(edgeID)) + edgeID
                self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_INTEGER, lane)    
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, x)
                self.traciInst._message.string += struct.pack("!Bd", tc.TYPE_DOUBLE, y)
                self.traciInst._sendExact()

