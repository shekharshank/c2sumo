package edu.vanderbilt.isis.trafficmap.valueobjects;

import android.os.Parcel;
import android.os.Parcelable;

public final class Vehicle implements Parcelable {
	public final String mId;
	public final double mLongitude;
	public final double mLatitude;

	public Vehicle(String id, double latitude, double longitude) {
		this.mId = id;
		this.mLongitude = longitude;
		this.mLatitude = latitude;
	}

	public Vehicle(Parcel in) {
		this.mId = in.readString();
		this.mLongitude = in.readDouble();
		this.mLatitude = in.readDouble();
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

	}

	public static final Parcelable.Creator<Vehicle> CREATOR = new Parcelable.Creator<Vehicle>() {
		public Vehicle createFromParcel(Parcel in) {
			return new Vehicle(in);
		}

		public Vehicle[] newArray(int size) {
			return new Vehicle[size];
		}
	};
}
