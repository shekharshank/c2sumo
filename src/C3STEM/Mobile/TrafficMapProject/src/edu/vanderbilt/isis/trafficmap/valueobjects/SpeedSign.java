package edu.vanderbilt.isis.trafficmap.valueobjects;

import android.os.Parcel;
import android.os.Parcelable;

public final class SpeedSign implements Parcelable {
	public final String mId;
	public final double mLongitude;
	public final double mLatitude;
	public final double mSpeedLimit;

	public SpeedSign(String id, double latitude, double longitude, double speed) {
		this.mId = id;
		this.mLongitude = longitude;
		this.mLatitude = latitude;
		this.mSpeedLimit = speed;
	}

	public SpeedSign(Parcel in) {
		this.mId = in.readString();
		this.mLongitude = in.readDouble();
		this.mLatitude = in.readDouble();
		this.mSpeedLimit = in.readDouble();
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
		dest.writeDouble(mSpeedLimit);
	}

	public static final Parcelable.Creator<SpeedSign> CREATOR = new Parcelable.Creator<SpeedSign>() {
		public SpeedSign createFromParcel(Parcel in) {
			return new SpeedSign(in);
		}

		public SpeedSign[] newArray(int size) {
			return new SpeedSign[size];
		}
	};
}

