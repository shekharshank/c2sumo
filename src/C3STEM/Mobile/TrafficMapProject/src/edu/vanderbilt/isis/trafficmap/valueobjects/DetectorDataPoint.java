package edu.vanderbilt.isis.trafficmap.valueobjects;

import android.os.Parcel;
import android.os.Parcelable;

public final class DetectorDataPoint implements Parcelable {
	public final long mTimestamp;
	public final int mNumberOfCars;
	public final double mMeanSpeed;

	public DetectorDataPoint(long timestamp, int numberOfCars, double meanSpeed) {
		this.mTimestamp = timestamp;
		this.mNumberOfCars = numberOfCars;
		this.mMeanSpeed = meanSpeed;
	}

	public DetectorDataPoint(Parcel in) {
		this.mTimestamp = in.readLong();
		this.mNumberOfCars = in.readInt();
		this.mMeanSpeed = in.readDouble();		
	}

	@Override
	public int describeContents() {
		return 0;
	}

	@Override
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeLong(mTimestamp);
		dest.writeInt(mNumberOfCars);
		dest.writeDouble(mMeanSpeed);
	}

	public static final Parcelable.Creator<DetectorDataPoint> CREATOR = new Parcelable.Creator<DetectorDataPoint>() {
		public DetectorDataPoint createFromParcel(Parcel in) {
			return new DetectorDataPoint(in);
		}

		public DetectorDataPoint[] newArray(int size) {
			return new DetectorDataPoint[size];
		}
	};
}

