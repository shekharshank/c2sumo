package edu.vanderbilt.isis.trafficmap;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import edu.vanderbilt.isis.trafficmap.valueobjects.Detector;
import edu.vanderbilt.isis.trafficmap.valueobjects.DetectorDataPoint;
import edu.vanderbilt.isis.trafficmap.valueobjects.SpeedSign;
import edu.vanderbilt.isis.trafficmap.valueobjects.TrafficDataVO;
import edu.vanderbilt.isis.trafficmap.valueobjects.Vehicle;

import android.util.Xml;

public class XMLParser {
	
	//private static int count = 0;
	//private static double interval = 0.001;

	// We don't use namespaces
	private static final String ns = null;
	
	private static final String ROOT_TAG = "trafficdata";
	private static final String VEHICLE_TAG = "vehicle";
	private static final String SPEEDSIGN_TAG = "speedsign";
	private static final String DETECTOR_TAG = "detector";
	private static final String DATAPOINT_TAG = "datapoint";
	private static final String ID_ATTRIBUTE = "id";
	private static final String LAT_ATTRIBUTE = "lat";
	private static final String LNG_ATTRIBUTE = "lng";
	private static final String AVG_SPEED_ATTRIBUTE = "meanspeed";
	private static final String NUMBER_OF_CARS_ATTRIBUTE = "numberofcars";
	private static final String TIME_ATTRIBUTE = "time";
	private static final String SPEED_LIMIT_ATTRIBUTE = "speedlimit";

	public TrafficDataVO parse(InputStream in) throws XmlPullParserException,
			IOException {
		try {
			XmlPullParser parser = Xml.newPullParser();
			parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, false);
			parser.setInput(in, null);
			parser.nextTag();
			return readData(parser);
		} finally {
			in.close();
		}
	}

	private TrafficDataVO readData(XmlPullParser parser)
			throws XmlPullParserException, IOException {
		List<Vehicle> vehicleEntries = new ArrayList<Vehicle>();
		List<SpeedSign> speedEntries = new ArrayList<SpeedSign>();
		List<Detector> detectorEntries = new ArrayList<Detector>();

		parser.require(XmlPullParser.START_TAG, ns, ROOT_TAG);
		//parser.require(XmlPullParser.START_TAG, ns, "vehicles");
		
		while (parser.next() != XmlPullParser.END_DOCUMENT) {
			if (parser.getEventType() != XmlPullParser.START_TAG) {
				continue;
			}
			String name = parser.getName();
			// Starts by looking for the vehicle entry tag
			if (name.equals(VEHICLE_TAG)) {
				Vehicle vehicle = readVehicle(parser);
				if(!(vehicle.mLatitude == 0 && vehicle.mLongitude == 0))
					vehicleEntries.add(vehicle);
			}
			
			// Starts by looking for the speedsign entry tag
			else if (name.equals(SPEEDSIGN_TAG)) {
				SpeedSign speedsign = readSpeedSign(parser);
				if(!(speedsign.mLatitude == 0 && speedsign.mLongitude == 0))
					speedEntries.add(speedsign);
			}
			
			// Starts by looking for the detector entry tag
			else if (name.equals(DETECTOR_TAG)) {
				Detector detector = readDetector(parser);
				if(!(detector.mLatitude == 0 && detector.mLongitude == 0))
					detectorEntries.add(detector);
			}
		}
		//count++;
		return new TrafficDataVO(vehicleEntries, speedEntries, detectorEntries);
	}

	private Vehicle readVehicle(XmlPullParser parser)
			throws XmlPullParserException, IOException {

		parser.require(XmlPullParser.START_TAG, ns, VEHICLE_TAG);
		String tag = parser.getName();

		Vehicle vehicle = null;

		if (tag.equals(VEHICLE_TAG)) {
			String id = parser.getAttributeValue(null, ID_ATTRIBUTE);
			String lat = parser.getAttributeValue(null, LAT_ATTRIBUTE);
			String lng = parser.getAttributeValue(null, LNG_ATTRIBUTE);
			vehicle = new Vehicle(id, Double.parseDouble(lat),
					Double.parseDouble(lng));
			//vehicle = new Vehicle(id, Double.parseDouble(lat) + count*interval,
				//	Double.parseDouble(lng) + count*interval);
		}
		parser.nextTag();
		parser.require(XmlPullParser.END_TAG, ns, VEHICLE_TAG);
		return vehicle;
	}
	
	private SpeedSign readSpeedSign(XmlPullParser parser)
			throws XmlPullParserException, IOException {

		parser.require(XmlPullParser.START_TAG, ns, SPEEDSIGN_TAG);
		String tag = parser.getName();

		SpeedSign speedSign = null;

		if (tag.equals(SPEEDSIGN_TAG)) {
			String id = parser.getAttributeValue(null, ID_ATTRIBUTE);
			String lat = parser.getAttributeValue(null, LAT_ATTRIBUTE);
			String lng = parser.getAttributeValue(null, LNG_ATTRIBUTE);
			String speedLimit = parser.getAttributeValue(null, SPEED_LIMIT_ATTRIBUTE);
			speedSign = new SpeedSign(id, Double.parseDouble(lat),
					Double.parseDouble(lng), Double.parseDouble(speedLimit));
		}
		parser.nextTag();
		parser.require(XmlPullParser.END_TAG, ns, SPEEDSIGN_TAG);
		return speedSign;
	}
	
	private Detector readDetector(XmlPullParser parser)
			throws XmlPullParserException, IOException {

		parser.require(XmlPullParser.START_TAG, ns, DETECTOR_TAG);

		String id = parser.getAttributeValue(null, ID_ATTRIBUTE);
		String lat = parser.getAttributeValue(null, LAT_ATTRIBUTE);
		String lng = parser.getAttributeValue(null, LNG_ATTRIBUTE);
		List<DetectorDataPoint> dataPointList = new ArrayList<DetectorDataPoint>();
		parser.next();
		while (!((parser.getName() != null) && parser.getName().equals(DETECTOR_TAG))) {
			if (parser.getEventType() != XmlPullParser.START_TAG) {
				parser.next();
				continue;
			}		
			parser.require(XmlPullParser.START_TAG, ns, DATAPOINT_TAG);
			String timestamp = parser.getAttributeValue(null, TIME_ATTRIBUTE);
			String numberOfCars = parser.getAttributeValue(null, NUMBER_OF_CARS_ATTRIBUTE);
			String meanSpeed = parser.getAttributeValue(null, AVG_SPEED_ATTRIBUTE);
			
			DetectorDataPoint dp = new DetectorDataPoint(Timestamp.valueOf(timestamp).getTime(), Integer.valueOf(numberOfCars),
					Double.parseDouble(meanSpeed));
			dataPointList.add(dp);
			parser.next();			
		}
		Detector detector = new Detector(id, Double.parseDouble(lat),
				Double.parseDouble(lng), dataPointList);
		return detector;
	}
}
