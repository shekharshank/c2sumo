package edu.vanderbilt.isis.trafficmap;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.GoogleMap.OnMarkerClickListener;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.LatLngBounds;
import com.google.android.gms.maps.model.LatLngBounds.Builder;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;

import edu.vanderbilt.isis.trafficmap.valueobjects.Detector;
import edu.vanderbilt.isis.trafficmap.valueobjects.SpeedSign;
import edu.vanderbilt.isis.trafficmap.valueobjects.TrafficDataVO;
import edu.vanderbilt.isis.trafficmap.valueobjects.Vehicle;

import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.os.Messenger;
import android.os.Parcelable;
import android.provider.MediaStore;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Bitmap;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Toast;

public class MainActivity extends android.support.v4.app.FragmentActivity
		implements OnMarkerClickListener {

	// EditText for use input URL
	private EditText intervalEditText;
	// EditText for use input URL
	private EditText serverEditText;
	// TAG used for logging
	private static final String TAG = "MainActivity";
	// ProgressDialog to be displayed while image is getting downloaded
	private ProgressDialog pd;
	// Context to be set with Activity context and used in Toast
	private Context context;

	private Intent serviceIntent;

	private GoogleMap mMap;

	private boolean mapPlotted;

	private boolean mapEditMode;
	
	private TrafficDataVO mTrafficData;
	
	private Map<Double, Integer> mSpeedImages;

	private int speedLimit = 50;
	
	private static boolean isLaunch = true; 
	
	// nahsville location
	private double lat = 36.1658;
	private double lng = -86.7844;

	final static String lineEnd = "\r\n";
	final static String twoHyphens = "--";
	final static String boundary = "*****####";
	//
	private static final int OPEN_CAMERA_REQUEST = 10;
	

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
		// extract elements to which need to be accessed later
		intervalEditText = (EditText) findViewById(R.id.interval_input);
		//
		serverEditText = (EditText) findViewById(R.id.server_input);
		// set global context
		context = MainActivity.this;

		serviceIntent = new Intent(this, ThreadedDownloadService.class);

		createSpeedMap();
		
		// Try to obtain the map from the SupportMapFragment.
		mMap = ((SupportMapFragment) getSupportFragmentManager()
				.findFragmentById(R.id.map)).getMap();
		mMap.moveCamera(CameraUpdateFactory.newLatLng(new LatLng(lat, lng)));
		mMap.setOnMarkerClickListener(new OnMarkerClickListener() {

			@Override
			public boolean onMarkerClick(Marker marker) {
				if(mapEditMode && marker.getTitle().equals("Set Speed")) {
					takeInput();
					return true;
				} else if(mapEditMode && marker.getTitle().equals("detector_0")) {
					drawMap();
					return true;
				}	
					
				return false;
			}
		});

	}
	
	private void drawMap() {
		Intent intent = new Intent(context, WebViewActivity.class);
		Detector detector = mTrafficData.detectorList.get(0);
		intent.putParcelableArrayListExtra(WebViewActivity.DETECTOR_KEY, (ArrayList<? extends Parcelable>) detector.mDetectorDataPointList);
	    startActivity(intent);
	}
	
	private void createSpeedMap() {
		mSpeedImages = new TreeMap<Double, Integer>();
		mSpeedImages.put(20.0, R.drawable.speed_20);
		mSpeedImages.put(30.0, R.drawable.speed_30);
		mSpeedImages.put(40.0, R.drawable.speed_40);
		mSpeedImages.put(50.0, R.drawable.speed_50);
		mSpeedImages.put(60.0, R.drawable.speed_60);
		mSpeedImages.put(70.0, R.drawable.speed_70);
	}

	private void takeInput() {
		// Display a dialog to take user input
		AlertDialog.Builder helpBuilder = new AlertDialog.Builder(this);
		helpBuilder.setTitle(context.getString(R.string.ENTER_MAX_SPEED_DIALOG_TITLE));

		LayoutInflater inflater = getLayoutInflater();
		// read the layout from XML
		final View syncSettingLayout = inflater.inflate(R.layout.editmaxspeed,
				null);
		((Spinner)syncSettingLayout.findViewById(R.id.speed_input)).setSelection(speedLimit/10 - 2);
		helpBuilder.setView(syncSettingLayout);

		// add a confirmation button
		helpBuilder.setPositiveButton("Ok",
				new DialogInterface.OnClickListener() {

					/**
					 * Perform setup here
					 */
					public void onClick(DialogInterface dialog, int which) {

						int speed = Integer
								.parseInt(((Spinner) syncSettingLayout
										.findViewById(R.id.speed_input))
										.getSelectedItem().toString());
						SpeedSetTask task = new SpeedSetTask();
						task.execute(speed);
						
						
					}
				});

		// Remember, create doesn't show the dialog
		AlertDialog helpDialog = helpBuilder.create();
		helpDialog.show();

	}

	private void plotMap(final TrafficDataVO trafficData) {
		// Hide the zoom controls as the button panel will cover it.
		// mMap.getUiSettings().setZoomControlsEnabled(false);

		// Add lots of markers to the map.
		addMarkersToMap(trafficData);

		if (!mapPlotted) {
			Builder builder = new LatLngBounds.Builder();
			List<Vehicle> vehicles = trafficData.vehicleList;
			for (Vehicle vehicle : vehicles) {
				builder.include(new LatLng(vehicle.mLatitude,
						vehicle.mLongitude));
			}
			List<SpeedSign> speedSigns = trafficData.speedLimitList;
			for (SpeedSign speedSign : speedSigns) {
				builder.include(new LatLng(speedSign.mLatitude,
						speedSign.mLongitude));
			}
			LatLngBounds bounds = builder.build();
			mMap.animateCamera(CameraUpdateFactory.newLatLngBounds(bounds, 80));
			mapPlotted = true;
		}

	}

	private void addMarkersToMap(TrafficDataVO trafficData) {

		mMap.clear();		
		for (Vehicle vehicle : trafficData.vehicleList) {			
			mMap.addMarker(new MarkerOptions()
					.position(new LatLng(vehicle.mLatitude, vehicle.mLongitude))
					.title(vehicle.mId)
					.icon(BitmapDescriptorFactory
							.fromResource(R.drawable.truck)));
		}
		
		for (SpeedSign speedSign : trafficData.speedLimitList) {
			mMap.addMarker(new MarkerOptions()
					.position(new LatLng(speedSign.mLatitude, speedSign.mLongitude))
					.title("Set Speed")
					.icon(BitmapDescriptorFactory
							.fromResource(
									mSpeedImages.get(speedSign.mSpeedLimit))));
			speedLimit = (int)speedSign.mSpeedLimit;
		}
		
		for (Detector detector : trafficData.detectorList) {			
			mMap.addMarker(new MarkerOptions()
					.position(new LatLng(detector.mLatitude, detector.mLongitude))
					.title(detector.mId).icon(BitmapDescriptorFactory
							.fromResource(R.drawable.trafficcamera)));
		}

	}

	@Override
	public boolean onMarkerClick(Marker marker) {
		Toast.makeText(context, "Marker: " + marker.getTitle(),
				Toast.LENGTH_LONG).show();
		return false;
	}

	public void editButtonChange(View view) {
		if (view.getId() == R.id.edit_radio_id) {
			mapEditMode = true;
			stopService(serviceIntent);
			//findViewById(R.id.button_layout_id).setVisibility(View.VISIBLE);
		} else {
			mapEditMode = false;
			//findViewById(R.id.button_layout_id).setVisibility(View.GONE);
			startService(serviceIntent);
		}
	}

	public void openCamera(View view) {
		Intent intent = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);
		startActivityForResult(intent, OPEN_CAMERA_REQUEST);
	}

	@Override
	public void onActivityResult(int requestCode, int resultCode, Intent data) {
		if (resultCode == Activity.RESULT_OK
				&& requestCode == OPEN_CAMERA_REQUEST) {
			Bitmap image = (Bitmap) data.getExtras().get("data");

			ServerTask task = new ServerTask();
			task.execute(image);

		}
	}

	public void runThreadedMessenger(View view) {
		final double interval = Double.parseDouble(intervalEditText.getText()
				.toString());

		final String server = serverEditText.getText().toString();

		findViewById(R.id.linear_layout).setVisibility(View.GONE);
		findViewById(R.id.edit_type_radio_id).setVisibility(View.VISIBLE);

		// display progress dialog
		pd = ProgressDialog.show(context,
				context.getString(R.string.PROGRESS_DIALOG_TITLE),
				context.getString(R.string.PROGRESS_DIALOG_MESSAGE_MESSENGER),
				true, false);
		Log.d(TAG, "Progress dialog displayed.");

		// create messenger and put in intent
		Messenger messenger = new Messenger(handler);
		serviceIntent.putExtra(ThreadedDownloadService.MESSENGER, messenger);
		// set url
		serviceIntent.putExtra(
				ThreadedDownloadService.URL_KEY,
				context.getString(R.string.URL_PREFIX) + server
						+ context.getString(R.string.DOWNLOAD_URL_SUFFIX));
		// set interval
		serviceIntent.putExtra(ThreadedDownloadService.INTERVAL_KEY, interval);
		// start service
		startService(serviceIntent);
		Log.d(TAG, "Thread download service started for Messenger.");
	}

	/**
	 * Handler used by messenger
	 */
	private Handler handler = new Handler() {

		@Override
		public void handleMessage(Message msg) {
			// extract the information sent by service
			Bundle data = msg.getData();
			if (data == null || mapEditMode) {
				displayErrorMessage();
				return;
			}
			
			Parcelable resultData = data.getParcelable(ThreadedDownloadService.RESULT_KEY);

			if (resultData == null) {
				displayErrorMessage();
				return;
			}

			/*
			List<Vehicle> vehicles = (TrafficDataVO)resultData).ve
			for (Parcelable parcel : ((TrafficDataVO)resultData).) {
				vehicles.add((Vehicle) parcel);
			}*/

			if (pd != null && pd.isShowing())
				pd.dismiss();
			
			isLaunch = false;

			plotMap((TrafficDataVO)resultData);
			// store a reference
			mTrafficData = (TrafficDataVO)resultData;
		}
	};

	/**
	 * Displays invalid URL error message using toast
	 */
	private void displayErrorMessage() {
		if(isLaunch)
			Toast.makeText(context, context.getString(R.string.INVALID_URL_ERROR),
				Toast.LENGTH_LONG).show();
		if (pd != null && pd.isShowing())
			pd.dismiss();
		//stopService(serviceIntent);
		isLaunch = false;
	}

	@Override
	public void onDestroy() {
		stopService(serviceIntent);
		super.onDestroy();
	}

	// *******************************************************************************
	// Push image to server
	// *******************************************************************************

	public class ServerTask extends AsyncTask<Bitmap, Integer, String> {
		private ProgressDialog dialog;

		public ServerTask() {
			dialog = new ProgressDialog(context);
		}

		protected void onPreExecute() {
			this.dialog.setMessage("Sending...");
			this.dialog.setTitle("Image Captured");
			this.dialog.show();
		}

		@Override
		protected String doInBackground(Bitmap... params) // background
															// operation
		{

			URL url;
			HttpURLConnection connection = null;
			try {
				final String server = serverEditText.getText().toString();
				// Create connection
				url = new URL(context.getString(R.string.URL_PREFIX) + server
						+ context.getString(R.string.UPLOAD_URL_SUFFIX));
				connection = (HttpURLConnection) url.openConnection();
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Connection", "Keep-Alive");

				connection.setRequestProperty("Content-Type",
						"multipart/form-data;boundary=" + boundary);

				connection.setUseCaches(false);
				connection.setDoInput(true);
				connection.setDoOutput(true);

				// Send request
				DataOutputStream wr = new DataOutputStream(
						connection.getOutputStream());
				wr.writeBytes(twoHyphens + boundary + lineEnd);

				//
				wr.writeBytes("Content-Disposition: form-data; name=\"myFile\";filename=\""
						+ "cameraimage.jpg" + "\"" + lineEnd);
				wr.writeBytes("Content-Type: image/jpeg" + lineEnd);
				wr.writeBytes(lineEnd);

				// create a buffer of maximum size
				params[0].compress(Bitmap.CompressFormat.JPEG, 100, wr);
				wr.writeBytes(lineEnd);
				wr.writeBytes(twoHyphens + boundary + twoHyphens + lineEnd);
				wr.writeBytes(lineEnd);
				wr.flush();
				wr.close();

				// System.out.println(connection.getResponseCode());

				// Get Response

				InputStream is = connection.getInputStream();
				BufferedReader rd = new BufferedReader(
						new InputStreamReader(is));
				String line;
				StringBuffer response = new StringBuffer();
				while ((line = rd.readLine()) != null) {
					response.append(line);
					response.append('\r');
				}
				rd.close();
				return (response.toString());

			} catch (Exception e) {

				Log.e(TAG, e.getMessage(), e);

			} finally {

				if (connection != null) {
					connection.disconnect();
				}
			}

			return null;
		}

		@Override
		protected void onPostExecute(String param) {
			if (dialog != null && dialog.isShowing()) {
				dialog.dismiss();
			}
			Toast.makeText(context, "Response from server: " + param,
					Toast.LENGTH_LONG).show();
		}

	}
	
	// *******************************************************************************
		// Set speed on server
		// *******************************************************************************

		public class SpeedSetTask extends AsyncTask<Integer, Integer, String> {
			private ProgressDialog dialog;

			public SpeedSetTask() {
				dialog = new ProgressDialog(context);
			}

			protected void onPreExecute() {
				this.dialog.setMessage("Sending...");
				this.dialog.setTitle("Speed set request");
				this.dialog.show();
			}

			@Override
			protected String doInBackground(Integer... params) // background
																// operation
			{

				URL url;
				HttpURLConnection connection = null;
				try {
					final String server = serverEditText.getText().toString();
					// Create connection
					url = new URL(context.getString(R.string.URL_PREFIX) + server
							+ context.getString(R.string.SET_MAX_SPEED_URL_SUFFIX) + "&maxSpeed=" + params[0]);
					speedLimit = params[0];
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setDoOutput(true);
			        connection.setReadTimeout(1000);
			        connection.connect();
			        
			      //read the result from the server
			        BufferedReader rd  = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			        StringBuilder  response = new StringBuilder();
			        String line;
					while ((line = rd.readLine()) != null) {
						response.append(line);
						response.append('\r');
					}
					rd.close();

					return (response.toString());

				} catch (Exception e) {

					Log.e(TAG, e.getMessage(), e);

				} finally {

					if (connection != null) {
						connection.disconnect();
					}
				}

				return null;
			}

			@Override
			protected void onPostExecute(String param) {
				if (dialog != null && dialog.isShowing()) {
					dialog.dismiss();
				}
								
				Toast.makeText(context, "Speed Set..",
						Toast.LENGTH_LONG).show();
				addMarkersToMap(mTrafficData);
			}

		}

}
