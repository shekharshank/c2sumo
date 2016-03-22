package edu.vanderbilt.isis.trafficmap;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;

import edu.vanderbilt.isis.trafficmap.valueobjects.TrafficDataVO;

import android.app.Service;

import android.content.Intent;
import android.util.Log;

import android.os.Bundle;
import android.os.IBinder;
import android.os.Message;
import android.os.Messenger;
import android.os.RemoteException;

/**
 * Service which downloads image from the input URL using 3 different mechanisms
 * and returns it to the Activity invoking it
 *
 *
 */
public class ThreadedDownloadService extends Service {

	// TAG used for logging
	private static final String TAG = "ThreadedDownloadService";

	// Key for returning result
	public static final String RESULT_KEY = "RESULT_KEY";
	// Key used for receiving URL input
	public static final String URL_KEY = "URL_KEY";
	// Key used for receiving interval input
	public static final String INTERVAL_KEY = "INTERVAL_KEY";
	// Key for inserting messenger in intent
	public static final String MESSENGER = "MESSENGER";

	public static final int CONN_TIMEOUT = 4000;

	private boolean running = true;

	@Override
	public IBinder onBind(Intent arg0) {
		// Not used
		return null;
	}

	/**
	 * Downloads the file from the specified url and returns the location of the
	 * downloaded file
	 *
	 * @param urlValue
	 * @return
	 * @throws IOException
	 */
	private TrafficDataVO downloadFile(String urlValue) throws IOException {

		Log.d(TAG, "Url to download:" + urlValue);

		XMLParser parser = new XMLParser();

		InputStream is = null;
		URLConnection conn = null;
		try {
			// Open an input stream to the xml
			conn = new URL(urlValue).openConnection();
			conn.setConnectTimeout(CONN_TIMEOUT);

		} catch (Exception e) {
			// log the exception
			Log.e(TAG, e.getMessage());
		}
		if(conn != null) {
			is = new BufferedInputStream(conn
					.getInputStream());
			Log.d(TAG, "Input stream opened.");
		}

		try {
			return parser.parse(is);
		} catch (Exception e) {
			// log the exception
			Log.e(TAG, e.getMessage());
		}
		return null;
	}

	/**
	 * Used by Messenger to reply the traffic data
	 *
	 * @param outputPath
	 * @param messenger
	 */
	private void sendTrafficData(TrafficDataVO trafficData, Messenger messenger) {
		Message msg = Message.obtain();
		if(trafficData != null) {
			Bundle bundle = new Bundle();
			// put output path in bundle
			bundle.putParcelable(RESULT_KEY, trafficData);
			msg.setData(bundle);
		}

		try {
			// send message
			messenger.send(msg);
			Log.d(TAG, "Service replied using messenger.");
		} catch (RemoteException e) {
			Log.e(TAG, e.getMessage(), e);
		}
	}

	/**
	 * Downloads image in new thread and replies using messenger
	 *
	 * @param intent
	 */
	private void threadMessageDownload(Intent intent) {
		final Messenger messenger = (Messenger) intent.getExtras().get(
				MESSENGER);
		Bundle extras = intent.getExtras();
		// extract url
		final String url = extras.getString(URL_KEY);
		// extract refresh interval
		final double interval = extras.getDouble(INTERVAL_KEY);

		// create new anonymous thread using Runnable for performing background
		// task
		new Thread(new Runnable() {
			public void run() {
				// Download xml
				while (running) {
					long lastTime = Calendar.getInstance().getTimeInMillis();
					TrafficDataVO trafficData = null;
					try {
						trafficData = downloadFile(url);
					} catch (IOException e1) {
						Log.e(TAG, e1.getMessage(), e1);
						sendTrafficData(trafficData, messenger);
						return;
					}
					// send path
					if (running) {
						sendTrafficData(trafficData, messenger);

						// sleep for the specified interval
						long presentTime = Calendar.getInstance()
								.getTimeInMillis();

						if (presentTime < (lastTime + interval * 1000)) {
							long elapsedTime = presentTime - lastTime;
							try {
								Thread.sleep((int)(interval * 1000) - elapsedTime);
							} catch (InterruptedException e) {
								Log.d(TAG, "Thread Interrupted");
							}
						}

					}

				}

			}
		}).start();

	}

	@Override
	public int onStartCommand(Intent intent, int flags, int startId) {
		Log.d(TAG, "on Start Command called.");
		// make sure it is not restarted service
		if (intent != null) {
			Bundle extras = intent.getExtras();
			if (extras != null) {
				// check if intent has messenger, invoke method accordingly
				if (extras.get(MESSENGER) != null)
					threadMessageDownload(intent);

			} else
				Log.e(TAG, "Does not contain any extras");
		}

		// will redeliver intent
		return Service.START_REDELIVER_INTENT;
	}

	@Override
	public void onDestroy() {
		running = false;
		super.onDestroy();

	}
}