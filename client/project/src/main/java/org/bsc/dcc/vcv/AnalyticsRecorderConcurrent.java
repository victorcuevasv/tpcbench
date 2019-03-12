package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AnalyticsRecorderConcurrent extends AnalyticsRecorder {
	
	public AnalyticsRecorderConcurrent(String testName, String system) {
		super(testName, system);
	}
	
	public void header() {
		String[] titles = {"STREAM", "QUERY", "SUCCESSFUL", "DURATION", "RESULTS_SIZE", "SYSTEM", 
				   "STARTDATE_EPOCH", "STOPDATE_EPOCH", "DURATION_MS", "STARTDATE", "STOPDATE"};
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < titles.length - 1; i++)
			builder.append(String.format("%-25s|", titles[i]));
		builder.append(String.format("%-25s", titles[titles.length-1]));
		logger.info(builder.toString());
	}
	
	public void record(QueryRecordConcurrent queryRecord) {
		int spaces = 25;
		String colFormat = "%-" + spaces + "s|";
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(colFormat, queryRecord.getStream()));
		builder.append(String.format(colFormat, queryRecord.getQuery()));
		builder.append(String.format(colFormat, queryRecord.isSuccessful()));
		long durationMs = queryRecord.getEndTime() - queryRecord.getStartTime();
		String durationFormatted = String.format("%.3f", ((double) durationMs / 1000.0d));
		builder.append(String.format(colFormat, durationFormatted));
		builder.append(String.format(colFormat, queryRecord.getResultsSize()));
		builder.append(String.format(colFormat, this.system));
		builder.append(String.format(colFormat, queryRecord.getStartTime()));
		builder.append(String.format(colFormat, queryRecord.getEndTime()));
		builder.append(String.format(colFormat, durationMs));
		Date startDate = new Date(queryRecord.getStartTime());
		String startDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate);
		builder.append(String.format(colFormat, startDateFormatted));
		Date endDate = new Date(queryRecord.getEndTime());
		String endDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate);
		builder.append(String.format("%-" + spaces + "s", endDateFormatted));
		logger.info(builder.toString());
	}

}


