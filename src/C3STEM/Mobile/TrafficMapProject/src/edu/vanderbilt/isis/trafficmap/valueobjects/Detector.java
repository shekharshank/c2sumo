package edu.vanderbilt.isis.trafficmap.valueobjects;

import java.util.ArrayList;
import java.util.List;

import android.os.Parcel;
import android.os.Parcelable;

public final class Detector implements Parcelable {
	public final String mId;
	public final double mLongitude;
	public final double mLatitude;
	public  List<DetectorDataPoint> mDetectorDataPointList;

	public Detector(String id, double latitude, double longitude, List<DetectorDataPoint> detectorDataPointList) {
		this.mId = id;
		this.mLongitude = longitude;
		this.mLatitude = latitude;
		this.mDetectorDataPointList = detectorDataPointList;
	}

	public Detector(Parcel in) {
		this.mId = in.readString();
		this.mLongitude = in.readDouble();
		this.mLatitude = in.readDouble();
		this.mDetectorDataPointList = new ArrayList<DetectorDataPoint>();
		in.readTypedList(this.mDetectorDataPointList , DetectorDataPoint.CREATOR);
	}

	@Override
	public int describeContents() {
		return 0;
	}

	@Override
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeString(mId);
		dest.writeDouble(mLongitude);
		dest.writeDouble(mLatitude);
		dest.writeList(mDetectorDataPointList);
	}

	public static final Parcelable.Creator<Detector> CREATOR = new Parcelable.Creator<Detector>() {
		public Detector createFromParcel(Parcel in) {
			return new Detector(in);
		}

		public Detector[] newArray(int size) {
			return new Detector[size];
		}
	};
	
	
}

