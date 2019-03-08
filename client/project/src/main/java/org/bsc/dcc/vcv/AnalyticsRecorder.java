package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnalyticsRecorder {
	
	protected static final Logger logger = LogManager.getLogger("AnalyticsLog");
	
	protected String system;
	
	public AnalyticsRecorder(String system) {
		this.system = system;
	}
	
	public void message(String msg) {
		logger.info(msg);
	}
	
	public void header() {
		String[] titles = {"QUERY", "SUCCESSFUL", "DURATION", "RESULTS_SIZE", "SYSTEM", 
						   "STARTDATE_EPOCH", "STOPDATE_EPOCH", "DURATION_MS", "STARTDATE", "STOPDATE"};
		StringBuilder builder = new StringBuilder();
		for(String title : titles)
			builder.append(String.format("%-25s|", title));
		logger.info(builder.toString());
	}
	
	public void record(QueryRecord queryRecord) {
		int spaces = 25;
		String colFormat = "%-" + spaces + "s|";
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(colFormat, queryRecord.getQuery()));
		builder.append(String.format(colFormat, queryRecord.isSuccessful()));
		long durationMs = queryRecord.getEndTime() - queryRecord.getStartTime();
		String durationFormatted = String.format("%.3f", ((double) durationMs / 1000.0d));
		builder.append(String.format(colFormat, durationFormatted));
		builder.append(String.format(colFormat, queryRecord.getResultsSize()));
		builder.append(String.format(colFormat, this.system));
		builder.append(String.format(colFormat, durationMs));
		builder.append(String.format(colFormat, queryRecord.getStartTime()));
		builder.append(String.format(colFormat, queryRecord.getEndTime()));
		Date startDate = new Date(queryRecord.getStartTime());
		String startDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate);
		builder.append(String.format(colFormat, startDateFormatted));
		Date endDate = new Date(queryRecord.getEndTime());
		String endDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate);
		builder.append(String.format("%-" + spaces + "s", endDateFormatted));
		logger.info(builder.toString());
	}
	
}
