package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AnalyticsRecorderConcurrent extends AnalyticsRecorder {
	
	public AnalyticsRecorderConcurrent(String system) {
		super(system);
	}
	
	public void header() {
		String[] titles = {"STREAM", "QUERY", "SUCCESSFUL", "STARTDATE_EPOCH", "STOPDATE_EPOCH",
				                 "DURATION_MS", "STARTDATE", "STOPDATE", "DURATION", "RESULTS_SIZE"};
		StringBuilder builder = new StringBuilder();
		for(String title : titles)
			builder.append(String.format("%-25s|", title));
		logger.info(builder.toString());
	}
	
	public void record(QueryRecordConcurrent queryRecord) {
		int spaces = 25;
		String colFormat = "%-" + spaces + "s|";
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(colFormat, queryRecord.getStream()));
		builder.append(String.format(colFormat, queryRecord.getQuery()));
		builder.append(String.format(colFormat, queryRecord.isSuccessful()));
		builder.append(String.format(colFormat, queryRecord.getStartTime()));
		builder.append(String.format(colFormat, queryRecord.getEndTime()));
		long durationMs = queryRecord.getEndTime() - queryRecord.getStartTime();
		builder.append(String.format(colFormat, durationMs));
		Date startDate = new Date(queryRecord.getStartTime());
		String startDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate);
		builder.append(String.format(colFormat, startDateFormatted));
		Date endDate = new Date(queryRecord.getEndTime());
		String endDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate);
		builder.append(String.format(colFormat, endDateFormatted));
		String durationFormatted = String.format("%.3f", ((double) durationMs / 1000.0d));
		builder.append(String.format(colFormat, durationFormatted));
		builder.append(String.format(colFormat, queryRecord.getResultsSize()));
		logger.info(builder.toString());
	}

}


