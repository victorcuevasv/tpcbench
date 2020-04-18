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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class AnalyzeTablesSparkCLI {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final boolean computeForCols;
	private final CommandLine commandLine;

	
	public AnalyzeTablesSparkCLI(String[] args) throws Exception {
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
			this.spark = SparkSession.builder().appName("TPC-DS Database Table Analysis")
						.enableHiveSupport()
						.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in AnalyzeTablesSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = this.commandLine.getOptionValue("main-work-dir");
		this.dbName = this.commandLine.getOptionValue("schema-name");
		this.resultsDir = this.commandLine.getOptionValue("results-dir");
		this.experimentName = this.commandLine.getOptionValue("experiment-name");
		this.system = this.commandLine.getOptionValue("system-name");
		
		this.test = this.commandLine.getOptionValue("tpcds-test", "analyze");
		String instanceStr = this.commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		String computeForColsStr = this.commandLine.getOptionValue("use-column-stats");
		this.computeForCols = Boolean.parseBoolean(computeForColsStr);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
	}

	
	public static void main(String[] args) {
		AnalyzeTablesSparkCLI application = null;
		try {
			application = new AnalyzeTablesSparkCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
		}
		application.analyzeTables();
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
			// Use i + 1 for index to match the order in the load test.
			// Note in the tables array above that the dbgen_version table
			// is not included, since it is skipped in the load test.
			analyzeTable(tables[i], this.computeForCols, i + 1);
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


