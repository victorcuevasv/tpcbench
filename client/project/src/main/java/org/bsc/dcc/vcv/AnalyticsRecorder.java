package org.bsc.dcc.vcv;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AnalyticsRecorder {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	protected final String workDir;
	protected final String folderName;
	protected final String experimentName;
	protected final String system;
	protected final String test;
	protected final int instance;
	private final BufferedWriter writer;
	
	
	public AnalyticsRecorder(String workDir, String folderName, String experimentName,
			String system, String test, int instance) {
		this.workDir = workDir;
		this.folderName = folderName;
		this.experimentName = experimentName;
		this.system = system;
		this.test = test;
		this.instance = instance;
		BufferedWriter writerTemp;
		try {
			File logFile = new File(this.workDir + "/" + this.folderName + "/analytics/" + 
						this.experimentName + "/" + this.test + "/" + this.instance + "/analytics.log");
			logFile.getParentFile().mkdirs();
			writerTemp = new BufferedWriter(new FileWriter(logFile));
		}
		catch (Exception e) {
			writerTemp = null;
			e.printStackTrace();
			this.logger.error(e);
		}
		this.writer = writerTemp;
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
			this.logger.error("Error in AnalyticsRecorder message.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	public void header() {
		String[] titles = {"QUERY", "RUN", "SUCCESSFUL", "DURATION", "RESULTS_SIZE", "SYSTEM", 
						   "STARTDATE_EPOCH", "STOPDATE_EPOCH", "DURATION_MS", "STARTDATE",
						   "STOPDATE", "TUPLES"};
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < titles.length - 1; i++)
			builder.append(String.format("%-25s|", titles[i]));
		builder.append(String.format("%-25s", titles[titles.length-1]));
		if( this.system.equals("bigquery") )
			builder.append(String.format("|%-25s", "BYTES_BILLED"));
		this.message(builder.toString());
	}
	
	
	public void record(QueryRecord queryRecord) {
		int spaces = 25;
		String colFormat = "%-" + spaces + "s|";
		StringBuilder builder = new StringBuilder();
		builder.append(String.format(colFormat, queryRecord.getQuery()));
		builder.append(String.format(colFormat, queryRecord.getRun()));
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
		builder.append(String.format(colFormat, endDateFormatted));
		builder.append(String.format("%-" + spaces + "s", queryRecord.getTuples()));
		if( queryRecord instanceof QueryRecordBigQuery)
			builder.append(String.format("%-" + spaces + "s", 
				((QueryRecordBigQuery)queryRecord).getBytesBilled()));
		this.message(builder.toString());
	}
	
	
	public void close() {
		try {
			this.writer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in AnalyticsRecorder close.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
}

