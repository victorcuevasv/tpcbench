package org.bsc.dcc.vcv.prot;

import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryRecordConcurrent extends QueryRecord {

	int stream;
	
	public QueryRecordConcurrent(int stream, int query) {
		super(query);
		this.stream = stream;
	}
	
	public int getStream() {
		return this.stream;
	}
	
	public void setStream(int stream) {
		this.stream = stream;
	}
	
	public String toString() {
		int spaces = 25;
		String colFormat = "%-" + spaces + "s|";
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(colFormat, getStream()));
		builder.append(String.format(colFormat, getQuery()));
		builder.append(String.format(colFormat, isSuccessful()));
		long durationMs = getEndTime() - getStartTime();
		String durationFormatted = String.format("%.3f", ((double) durationMs / 1000.0d));
		builder.append(String.format(colFormat, durationFormatted));
		builder.append(String.format(colFormat, getResultsSize()));
		builder.append(String.format(colFormat, "prot"));
		builder.append(String.format(colFormat, getStartTime()));
		builder.append(String.format(colFormat, getEndTime()));
		builder.append(String.format(colFormat, durationMs));
		Date startDate = new Date(getStartTime());
		String startDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate);
		builder.append(String.format(colFormat, startDateFormatted));
		Date endDate = new Date(getEndTime());
		String endDateFormatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate);
		builder.append(String.format(colFormat, endDateFormatted));
		builder.append(String.format("%-" + spaces + "s", getTuples()));
		return builder.toString();
	}
}

