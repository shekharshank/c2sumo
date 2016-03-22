package edu.vanderbilt.isis.trafficmap;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import edu.vanderbilt.isis.trafficmap.valueobjects.Detector;
import edu.vanderbilt.isis.trafficmap.valueobjects.DetectorDataPoint;
import edu.vanderbilt.isis.trafficmap.valueobjects.Vehicle;
import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.webkit.WebView;

public class WebViewActivity extends Activity {
	private WebView webView;

	public static final String DETECTOR_KEY = "DETECTOR_KEY";

	private static final String TAG = "WebViewActivity";

	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.webview);

		webView = (WebView) findViewById(R.id.webView1);
		webView.getSettings().setJavaScriptEnabled(true);
		StringBuilder sb = new StringBuilder();
		sb.append("<html>");
		sb.append("<head>");
		sb.append("<script type=\"text/javascript\" src=\"https://www.google.com/jsapi\"></script>");
		sb.append("<script type=\"text/javascript\">      google.load('visualization', '1.0', {'packages':['corechart']});");
		sb.append("google.setOnLoadCallback(drawChart); ");
		sb.append("function drawChart() {");
		sb.append("var data = new google.visualization.DataTable();");
		sb.append("data.addColumn('string', 'Time');");
		sb.append("data.addColumn('number', 'No. of Cars');");
		sb.append("data.addColumn('number', 'Mean Speed');");

		ArrayList<DetectorDataPoint> detectorData = getIntent()
				.getParcelableArrayListExtra(DETECTOR_KEY);
		sb.append("data.addRows([");

		boolean firstUse = true;
		for (DetectorDataPoint dataPoint : detectorData) {
			if (!firstUse) {
				sb.append(",");
				firstUse = false;
			}
			firstUse = false;
			sb.append("['")
					.append(new Timestamp(dataPoint.mTimestamp).toString())
					.append("', ").append(dataPoint.mNumberOfCars)
					.append(", ").append(dataPoint.mMeanSpeed)
					.append("]");
		}
		sb.append("]);");
		sb.append("var options = {'title':'Detector Data'};");
		sb.append("var chart = new google.visualization.LineChart(document.getElementById('chart_div'));");
		sb.append("chart.draw(data, options);  }</script>");
		sb.append("</head>");
		sb.append("<body>");
		sb.append("<div id=\"chart_div\"></div>");
		sb.append("</body>");
		sb.append("</html>");
		webView.loadData(sb.toString(), "text/html", "UTF-8");

	}
}
