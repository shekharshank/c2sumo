package edu.vanderbilt.isis.trafficmap.valueobjects;

import java.util.ArrayList;
import java.util.List;

import android.os.Parcel;
import android.os.Parcelable;

public class TrafficDataVO implements Parcelable {

	public  List<Vehicle> vehicleList;
	public  List<SpeedSign> speedLimitList;
	public  List<Detector> detectorList;
	
	public TrafficDataVO(List<Vehicle> vehicleListIn, List<SpeedSign> speedLimitListIn
			, List<Detector> detectorListIn) {
		this.vehicleList = vehicleListIn;
		this.speedLimitList = speedLimitListIn;
		this.detectorList = detectorListIn;
	}
	
	public TrafficDataVO(Parcel in) {
		this.vehicleList = new ArrayList<Vehicle>();
		this.speedLimitList = new ArrayList<SpeedSign>();
		this.detectorList = new ArrayList<Detector>();
		in.readTypedList(this.vehicleList , Vehicle.CREATOR);
		in.readTypedList(this.speedLimitList , SpeedSign.CREATOR);
		in.readTypedList(this.detectorList , Detector.CREATOR);
	}
	
	@Override
	public int describeContents() {
		return 0;
	}

	@Override
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeList(vehicleList);
		dest.writeList(speedLimitList);
		dest.writeList(detectorList);
	}
	
	public static final Parcelable.Creator<TrafficDataVO> CREATOR = new Parcelable.Creator<TrafficDataVO>() {
		public TrafficDataVO createFromParcel(Parcel in) {
			return new TrafficDataVO(in);
		}

		public TrafficDataVO[] newArray(int size) {
			return new TrafficDataVO[size];
		}
	};
}