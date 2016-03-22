# -*- coding: utf-8 -*-
"""
@file    simulation.py
@author  Michael Behrisch
@date    2011-03-15
@version $Id: simulation.py 15340 2013-12-21 12:41:10Z namdre $

Python implementation of the self.traciInst interface.

SUMO, Simulation of Urban MObility; see http://sumo-sim.org/
Copyright (C) 2008-2013 DLR (http://www.dlr.de/) and contributors

This file is part of SUMO.
SUMO is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.
"""
import struct
from traciclass import Storage, SubscriptionResults
import constants as tc


class Simulation:
                    
                    def __init__(self,  traciInst):  
                        self.traciInst = traciInst
    
                        self._RETURN_VALUE_FUNC = {tc.VAR_TIME_STEP:     Storage.readInt,
                          tc.VAR_LOADED_VEHICLES_NUMBER:            Storage.readInt,
                          tc.VAR_LOADED_VEHICLES_IDS:               Storage.readStringList,
                          tc.VAR_DEPARTED_VEHICLES_NUMBER:          Storage.readInt,
                          tc.VAR_DEPARTED_VEHICLES_IDS:             Storage.readStringList,
                          tc.VAR_ARRIVED_VEHICLES_NUMBER:           Storage.readInt,
                          tc.VAR_ARRIVED_VEHICLES_IDS:              Storage.readStringList,
                          tc.VAR_PARKING_STARTING_VEHICLES_NUMBER:  Storage.readInt,
                          tc.VAR_PARKING_STARTING_VEHICLES_IDS:     Storage.readStringList,
                          tc.VAR_PARKING_ENDING_VEHICLES_NUMBER:    Storage.readInt,
                          tc.VAR_PARKING_ENDING_VEHICLES_IDS:       Storage.readStringList,
                          tc.VAR_STOP_STARTING_VEHICLES_NUMBER:     Storage.readInt,
                          tc.VAR_STOP_STARTING_VEHICLES_IDS:        Storage.readStringList,
                          tc.VAR_STOP_ENDING_VEHICLES_NUMBER:       Storage.readInt,
                          tc.VAR_STOP_ENDING_VEHICLES_IDS:          Storage.readStringList,
                          tc.VAR_MIN_EXPECTED_VEHICLES:             Storage.readInt,
                          tc.VAR_BUS_STOP_WAITING:                  Storage.readInt,
                          tc.VAR_TELEPORT_STARTING_VEHICLES_NUMBER: Storage.readInt,
                          tc.VAR_TELEPORT_STARTING_VEHICLES_IDS:    Storage.readStringList,
                          tc.VAR_TELEPORT_ENDING_VEHICLES_NUMBER:   Storage.readInt,
                          tc.VAR_TELEPORT_ENDING_VEHICLES_IDS:      Storage.readStringList,
                          tc.VAR_DELTA_T:                           Storage.readInt,
                          tc.VAR_NET_BOUNDING_BOX:                  lambda result: (result.read("!dd"), result.read("!dd"))}
                      
                        self.subscriptionResults = SubscriptionResults(self._RETURN_VALUE_FUNC)
                    

                    def _getUniversal(self, varID):
                        result = self.traciInst._sendReadOneStringCmd(tc.CMD_GET_SIM_VARIABLE, varID, "")
                        return self._RETURN_VALUE_FUNC[varID](result)
                    
                    def getCurrentTime(self):
                        """getCurrentTime() -> integer
                        
                        Returns the current simulation time in ms.
                        """
                        return self._getUniversal(tc.VAR_TIME_STEP)
                    
                    def getLoadedNumber(self):
                        """getLoadedNumber() -> integer
                        
                        Returns the number of vehicles which were loaded in this time step.
                        """
                        return self._getUniversal(tc.VAR_LOADED_VEHICLES_NUMBER)
                    
                    def getLoadedIDList(self):
                        """getLoadedIDList() -> list(string)
                        
                        Returns a list of ids of vehicles which were loaded in this time step. 
                        """
                        return self._getUniversal(tc.VAR_LOADED_VEHICLES_IDS)
                    
                    def getDepartedNumber(self):
                        """getDepartedNumber() -> integer
                        
                        Returns the number of vehicles which departed (were inserted into the road network) in this time step.
                        """
                        return self._getUniversal(tc.VAR_DEPARTED_VEHICLES_NUMBER)
                    
                    def getDepartedIDList(self):
                        """getDepartedIDList() -> list(string)
                        
                        Returns a list of ids of vehicles which departed (were inserted into the road network) in this time step. 
                        """
                        return self._getUniversal(tc.VAR_DEPARTED_VEHICLES_IDS)
                    
                    def getArrivedNumber(self):
                        """getArrivedNumber() -> integer
                        
                        Returns the number of vehicles which arrived (have reached their destination and are removed from the road network) in this time step. 
                        """
                        return self._getUniversal(tc.VAR_ARRIVED_VEHICLES_NUMBER)
                    
                    def getArrivedIDList(self):
                        """getArrivedIDList() -> list(string)
                        
                        Returns a list of ids of vehicles which arrived (have reached their destination and are removed from the road network) in this time step. 
                        """
                        return self._getUniversal(tc.VAR_ARRIVED_VEHICLES_IDS)
                    
                    def getParkingStartingVehiclesNumber(self):
                        """getParkingStartingVehiclesNumber() -> integer
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_PARKING_STARTING_VEHICLES_NUMBER)   
                    
                    def getParkingStartingVehiclesIDList(self):
                        """getParkingStartingVehiclesIDList() -> list(string)
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_PARKING_STARTING_VEHICLES_IDS)
                    
                    def getParkingEndingVehiclesNumber(self):
                        """getParkingEndingVehiclesNumber() -> integer
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_PARKING_ENDING_VEHICLES_NUMBER)   
                    
                    def getParkingEndingVehiclesIDList(self):
                        """getParkingEndingVehiclesIDList() -> list(string)
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_PARKING_ENDING_VEHICLES_IDS)
                     
                    def getStopStartingVehiclesNumber(self):
                        """getStopStartingVehiclesNumber() -> integer
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_STOP_STARTING_VEHICLES_NUMBER)   
                    
                    def getStopStartingVehiclesIDList(self):
                        """getStopStartingVehiclesIDList() -> list(string)
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_STOP_STARTING_VEHICLES_IDS)
                    
                    def getStopEndingVehiclesNumber(self):
                        """getStopEndingVehiclesNumber() -> integer
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_STOP_ENDING_VEHICLES_NUMBER)   
                    
                    def getStopEndingVehiclesIDList(self):
                        """getStopEndingVehiclesIDList() -> list(string)
                        
                        . 
                        """
                        return self._getUniversal(tc.VAR_STOP_ENDING_VEHICLES_IDS)
                        
                    def getMinExpectedNumber(self):
                        """getMinExpectedNumber() -> integer
                        
                        Returns the number of vehicles which are in the net plus the
                        ones still waiting to start. This number may be smaller than
                        the actual number of vehicles still to come because of delayed
                        route file parsing. If the number is 0 however, it is
                        guaranteed that all route files have been parsed completely
                        and all vehicles have left the network.
                        """
                        return self._getUniversal(tc.VAR_MIN_EXPECTED_VEHICLES)
                    
                    def getBusStopWaiting(self):
                        """getBusStopWaiting() -> integer
                        
                        .
                        """
                        return self._getUniversal(tc.VAR_BUS_STOP_WAITING)    
                        
                    def getStartingTeleportNumber(self):
                        """getStartingTeleportNumber() -> integer
                        
                        Returns the number of vehicles which started to teleport in this time step. 
                        """
                        return self._getUniversal(tc.VAR_TELEPORT_STARTING_VEHICLES_NUMBER)
                    
                    def getStartingTeleportIDList(self):
                        """getStartingTeleportIDList() -> list(string)
                        
                        Returns a list of ids of vehicles which started to teleport in this time step. 
                        """
                        return self._getUniversal(tc.VAR_TELEPORT_STARTING_VEHICLES_IDS)
                    
                    def getEndingTeleportNumber(self):
                        """getEndingTeleportNumber() -> integer
                        
                        Returns the number of vehicles which ended to be teleported in this time step. 
                        """
                        return self._getUniversal(tc.VAR_TELEPORT_ENDING_VEHICLES_NUMBER)
                    
                    def getEndingTeleportIDList(self):
                        """getEndingTeleportIDList() -> list(string)
                        
                        Returns a list of ids of vehicles which ended to be teleported in this time step. 
                        """
                        return self._getUniversal(tc.VAR_TELEPORT_ENDING_VEHICLES_IDS)
                    
                    def getDeltaT(self):
                        """getDeltaT() -> integer
                        
                        .
                        """
                        return self._getUniversal(tc.VAR_DELTA_T)
                    
                    def getNetBoundary(self):
                        """getNetBoundary() -> ((double, double), (double, double))
                        
                        The boundary box of the simulation network.
                        """
                        return self._getUniversal(tc.VAR_NET_BOUNDING_BOX)
                    
                    def convert2D(self, edgeID, pos, laneIndex=0, toGeo=False):
                        posType = tc.POSITION_2D
                        if toGeo:
                            posType = tc.POSITION_LON_LAT
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "", 1+4 + 1+4+len(edgeID)+8+1 + 1+1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 2)
                        self.traciInst._message.string += struct.pack("!Bi", tc.POSITION_ROADMAP, len(edgeID)) + edgeID
                        self.traciInst._message.string += struct.pack("!dBBB", pos, laneIndex, tc.TYPE_UBYTE, posType)
                        return self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "").read("!dd")
                    
                    def convert3D(self, edgeID, pos, laneIndex=0, toGeo=False):
                        posType = tc.POSITION_3D
                        if toGeo:
                            posType = tc.POSITION_LON_LAT_ALT
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "", 1+4 + 1+4+len(edgeID)+8+1 + 1+1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 2)
                        self.traciInst._message.string += struct.pack("!Bi", tc.POSITION_ROADMAP, len(edgeID)) + edgeID
                        self.traciInst._message.string += struct.pack("!dBBB", pos, laneIndex, tc.TYPE_UBYTE, posType)
                        return self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "").read("!ddd")
                    
                    def convertRoad(self, x, y, isGeo=False):
                        posType = tc.POSITION_2D
                        if isGeo:
                            posType = tc.POSITION_LON_LAT
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "", 1+4 + 1+8+8 + 1+1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 2)
                        self.traciInst._message.string += struct.pack("!Bdd", posType, x, y)
                        self.traciInst._message.string += struct.pack("!BB", tc.TYPE_UBYTE, tc.POSITION_ROADMAP)
                        result = self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "")
                        return result.readString(), result.readDouble(), result.read("!B")[0]
                    
                    def convertGeo(self, x, y, fromGeo=False):
                        fromType = tc.POSITION_2D
                        toType = tc.POSITION_LON_LAT
                        if fromGeo:
                            fromType = tc.POSITION_LON_LAT
                            toType = tc.POSITION_2D
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "", 1+4 + 1+8+8 + 1+1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 2)
                        self.traciInst._message.string += struct.pack("!Bdd", fromType, x, y)
                        self.traciInst._message.string += struct.pack("!BB", tc.TYPE_UBYTE, toType)
                        return self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.POSITION_CONVERSION, "").read("!dd")
                    
                    def getDistance2D(self, x1, y1, x2, y2, isGeo=False, isDriving=False):
                        """getDistance2D(double, double, double, double, boolean, boolean) -> double
                        
                        Reads two coordinate pairs and an indicator whether the air or the driving distance shall be computed. Returns the according distance.
                        """
                        posType = tc.POSITION_2D
                        if isGeo:
                            posType = tc.POSITION_LON_LAT
                        distType = tc.REQUEST_AIRDIST
                        if isDriving:
                            distType = tc.REQUEST_DRIVINGDIST
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.DISTANCE_REQUEST, "", 1+4 + 1+8+8 + 1+8+8 + 1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 3)
                        self.traciInst._message.string += struct.pack("!Bdd", posType, x1, y1)
                        self.traciInst._message.string += struct.pack("!BddB", posType, x2, y2, distType)
                        return self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.DISTANCE_REQUEST, "").readDouble()
                    
                    def getDistanceRoad(self, edgeID1, pos1, edgeID2, pos2, isDriving=False):
                        """getDistanceRoad(string, double, string, double, boolean) -> double
                        
                        Reads two positions on the road network and an indicator whether the air or the driving distance shall be computed. Returns the according distance.
                        """
                        distType = tc.REQUEST_AIRDIST
                        if isDriving:
                            distType = tc.REQUEST_DRIVINGDIST
                        self.traciInst._beginMessage(tc.CMD_GET_SIM_VARIABLE, tc.DISTANCE_REQUEST, "", 1+4 + 1+4+len(edgeID1)+8+1 + 1+4+len(edgeID2)+8+1 + 1)
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_COMPOUND, 3)
                        self.traciInst._message.string += struct.pack("!Bi", tc.POSITION_ROADMAP, len(edgeID1)) + edgeID1
                        self.traciInst._message.string += struct.pack("!dBBi", pos1, 0, tc.POSITION_ROADMAP, len(edgeID2)) + edgeID2
                        self.traciInst._message.string += struct.pack("!dBB", pos2, 0, distType)
                        return self.traciInst._checkResult(tc.CMD_GET_SIM_VARIABLE, tc.DISTANCE_REQUEST, "").readDouble()
                    
                    
                    def subscribe(self, varIDs=(tc.VAR_DEPARTED_VEHICLES_IDS,), begin=0, end=2**31-1):
                        """subscribe(list(integer), double, double) -> None
                        
                        Subscribe to one or more simulation values for the given interval.
                        """
                        self.traciInst._subscribe(tc.CMD_SUBSCRIBE_SIM_VARIABLE, begin, end, "x", varIDs)
                    
                    def getSubscriptionResults(self):
                        """getSubscriptionResults() -> dict(integer: <value_type>)
                        
                        Returns the subscription results for the last time step.
                        It is not possible to retrieve older subscription results than the ones
                        from the last time step.
                        """
                        return self.subscriptionResults.get("x")
                    
                    
                    def clearPending(self, routeID=""):
                        self.traciInst._beginMessage(tc.CMD_SET_SIM_VARIABLE, tc.CMD_CLEAR_PENDING_VEHICLES, "",
                                            1+4+len(routeID))
                        self.traciInst._message.string += struct.pack("!Bi", tc.TYPE_STRING, len(routeID)) + routeID
                        self.traciInst._sendExact()
