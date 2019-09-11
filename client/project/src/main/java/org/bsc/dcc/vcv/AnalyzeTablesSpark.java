package org.bsc.dcc.vcv;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;

public class AnalyzeTablesSpark {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String folderName;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final boolean computeForCols;

	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * 
	 * args[5] test name (i.e. analyze)
	 * args[6] experiment instance number
	 * args[7] compute statistics for columns (true/false)
	 * 
	 */
	public AnalyzeTablesSpark(String[] args) {
		this.workDir = args[0];
		this.dbName = args[1];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.computeForCols = Boolean.parseBoolean(args[7]);
		this.recorder = new AnalyticsRecorder(this.workDir, this.folderName, this.experimentName,
				this.system, this.test, this.instance);
		try {
			if( this.system.equals("sparkdatabricks") ) {
				this.spark = SparkSession.builder().appName("TPC-DS Database Table Analysis")
						//	.enableHiveSupport()
						.getOrCreate();
				//this.logger.info(SparkUtil.stringifySparkConfiguration(this.spark));
			}
			else {
				this.spark = SparkSession.builder().appName("TPC-DS Database Table Analysis")
						.enableHiveSupport()
						.getOrCreate();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in AnalyzeTablesSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	
	public static void main(String[] args) {
		if( args.length != 8 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		boolean computeForCols = Boolean.parseBoolean(args[2]);
		AnalyzeTablesSpark prog = new AnalyzeTablesSpark(args);
		prog.analyzeTables();
		//prog.closeConnection();
	}
	
	
	private void analyzeTables() {
		this.useDatabase(this.dbName);
		this.recorder.header();
		String[] tables = {"call_center", "catalog_page", "catalog_returns", "catalog_sales",
				"customer", "customer_address", "customer_demographics", "date_dim",
				"household_demographics", "income_band", "inventory", "item",
				"promotion", "reason", "ship_mode", "store", "store_returns",
				"store_sales", "time_dim", "warehouse", "web_page", "web_returns",
				"web_sales", "web_site"};
		for(int i = 0; i < tables.length; i++) {
			analyzeTable(tables[i], this.computeForCols, i);
		}
		this.recorder.close();
	}

	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in AnalyzeTablesSpark useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void analyzeTable(String tableName, boolean computeForCols, int index) {
		QueryRecord queryRecord = null;
		try {
			System.out.println("Analyzing table: " + tableName);
			this.logger.info("Analyzing table: " + tableName);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if (tableName.equals("dbgen_version")) {
				System.out.println("Skipping: " + tableName);
				return;
			}
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			if( computeForCols ) {
				Dataset<Row> dataset = this.spark.sql("DESCRIBE " + tableName);
				String columnsStr = processResults(dataset);
				this.spark.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS FOR COLUMNS " + 
						columnsStr);
			}
			else
				this.spark.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS");
			queryRecord.setSuccessful(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String processResults(Dataset<Row> dataset) {
		String retVal = null;
		try {
			List<String> list = dataset.map(row -> row.getString(0), Encoders.STRING()).collectAsList();
			String columnsStr = list.stream().map(x -> x).filter(s -> ! s.startsWith("#")).collect(Collectors.joining(", "));
			retVal = columnsStr;
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error processing results.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return retVal;
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

	
}


