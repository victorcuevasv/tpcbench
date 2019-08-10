package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AnalyticsRecorder {
	
	private static final Logger logger = LogManager.getLogger("AllLog");
	
	protected String testName;
	protected String system;
	protected String workDir;
	protected String folderName;
	protected String experimentName;
	protected int instance;
	private BufferedWriter writer;
	
	public AnalyticsRecorder(String testName, String system, String workDir, String folderName,
			String experimentName, int instance) {
		this.testName = testName;
		this.system = system;
		this.workDir = workDir;
		this.folderName = folderName;
		this.experimentName = experimentName;
		this.instance = instance;
		this.writer = null;
		try {
			File logFile = new File(this.workDir + "/" + this.folderName + "/logs/" + 
						this.experimentName + "/" + this.testName + "/" + this.instance + "/analytics.log");
			logFile.getParentFile().mkdirs();
			this.writer = new BufferedWriter(new FileWriter(logFile));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	protected void message(String msg) {
		//logger.info(msg);
		try {
			this.writer.write(msg);
			this.writer.newLine();
			this.writer.flush();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	public void header() {
		String[] titles = {"QUERY", "SUCCESSFUL", "DURATION", "RESULTS_SIZE", "SYSTEM", 
						   "STARTDATE_EPOCH", "STOPDATE_EPOCH", "DURATION_MS", "STARTDATE", "STOPDATE"};
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < titles.length - 1; i++)
			builder.append(String.format("%-25s|", titles[i]));
		builder.append(String.format("%-25s", titles[titles.length-1]));
		this.message(builder.toString());
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
		this.message(builder.toString());
	}
	
	
}
