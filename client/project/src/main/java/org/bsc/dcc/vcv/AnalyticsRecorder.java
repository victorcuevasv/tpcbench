package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.*;
import org.apache.logging.log4j.core.config.*;
import org.apache.logging.log4j.core.layout.*;

public class AnalyticsRecorder {
	
	protected static final Logger logger = LogManager.getLogger("AnalyticsLog");
	
	protected String testName;
	protected String system;
	
	public AnalyticsRecorder(String testName, String system) {
		this.testName = testName;
		this.system = system;
		this.addAppender(testName, system);
	}
	
	public void message(String msg) {
		logger.info(msg);
	}
	
	public void header() {
		String[] titles = {"QUERY", "SUCCESSFUL", "DURATION", "RESULTS_SIZE", "SYSTEM", 
						   "STARTDATE_EPOCH", "STOPDATE_EPOCH", "DURATION_MS", "STARTDATE", "STOPDATE"};
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < titles.length - 1; i++)
			builder.append(String.format("%-25s|", titles[i]));
		builder.append(String.format("%-25s", titles[titles.length-1]));
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
	
	public void addAppender(String testName, String system) {
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		PatternLayout layout = PatternLayout.newBuilder()
	            .withConfiguration(ctx.getConfiguration())
	            .withPattern("%msg%n").build();
		FileAppender appender = FileAppender.newBuilder()
				.withLayout(layout)
		        .withFileName( "/dbfs/data/logs/" + testName + "/" + system + "/analytics.log")
		        //.withFileName( "/data/logs/" + testName + "/" + system + "/analytics.log")
		        .withName("AnalyticsLogFilePower")
		        .withAppend(false)
		        .build();
		appender.start();
		config.addAppender(appender);
		updateLoggers(appender, config);
	}

	private void updateLoggers(final Appender appender, final Configuration config) {
		LoggerConfig loggerConfig = config.getLoggerConfig("AnalyticsLog");
		loggerConfig.addAppender(appender, null, null);
	}
	
}
