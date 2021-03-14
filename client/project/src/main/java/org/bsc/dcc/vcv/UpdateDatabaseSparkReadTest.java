package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Optional;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class UpdateDatabaseSparkReadTest {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String queriesDir;
	private final Optional<String> extTablePrefixCreated;
	private final String format;
	private final boolean doCount;
	private final boolean partition;
	private final String jarFile;
	private final String createSingleOrAll;
	private final String denormSingleOrAll;
	private final Map<String, String> precombineKeys;
	private final Map<String, String> primaryKeys;
	private final String customerSK;
	private final String hudiFileSize;
	private final boolean hudiUseMergeOnRead;
	private final String querySingleOrAll;
	private final int readInstance;
	
	public UpdateDatabaseSparkReadTest(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkReadTest constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		String readInstanceStr = commandLine.getOptionValue("read-instance", "0");
		this.readInstance = Integer.parseInt(readInstanceStr);
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		//this.test = commandLine.getOptionValue("tpcds-test", "loadupdate");
		this.test = "readtest" + this.readInstance;
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.queriesDir = "ReadTestQueries";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		//this.format = commandLine.getOptionValue("table-format");
		if( this.system.equalsIgnoreCase("sparkdatabricks") )
			this.format = "delta";
		else
			this.format = commandLine.getOptionValue("update-table-format", "hudi");
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.denormSingleOrAll = commandLine.getOptionValue("denorm-all-or-file", "all");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.querySingleOrAll = commandLine.getOptionValue("all-or-query-file", "all");
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.primaryKeys = new HudiPrimaryKeys().getMap();
		this.customerSK = commandLine.getOptionValue("gdpr-customer-sk");
		this.hudiFileSize = commandLine.getOptionValue("hudi-file-max-size", "1073741824");
		String hudiUseMergeOnReadStr = commandLine.getOptionValue("hudi-merge-on-read", "true");
		this.hudiUseMergeOnRead = Boolean.parseBoolean(hudiUseMergeOnReadStr);
	}
	

	public static void main(String[] args) throws SQLException {
		UpdateDatabaseSparkReadTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseSparkReadTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseSparkReadTest(commandLine);
		application.runQueries();
	}
	
	
	private void runQueries() {
		// Process each .sql create table file found in the jar file.
		this.useDatabase(this.dbName);
		this.recorder.header();
		List<String> list = this.queriesReader.getFilesOrdered();
		int i = 1;
		for (final String fileName : list) {
			String sqlQuery = this.queriesReader.getFile(fileName);
			if( ! this.querySingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.querySingleOrAll) )
					continue;
			}
			if( this.format.equals("delta") ) {
				runQueryDelta(fileName, sqlQuery, i);
				
			}
			else if( this.format.equals("hudi") ) {
				runQueryHudi(fileName, sqlQuery, i);
			}
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}

	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkReadTest useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void runQueryDelta(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			sqlQuery = sqlQuery.replace("<SUFFIX>", "_delta");
			String nQueryStr = sqlFilename.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			System.out.println("Processing query " + index + ": " + sqlFilename);
			this.logger.info("Processing query " + index + ": " + sqlFilename);
			queryRecord = new QueryRecord(nQuery);
			queryRecord.setStartTime(System.currentTimeMillis());
			Dataset<Row> resultDS = this.spark.sql(sqlQuery);
			queryRecord.setSuccessful(true);
			String resFileName = this.workDir + "/" + this.resultsDir + "/readresults" +
					this.readInstance + "/" + this.experimentName + "/" + this.instance +
					"/" + sqlFilename + ".txt";
			int tuples = this.saveResults(resFileName, resultDS, false);
			queryRecord.setTuples(queryRecord.getTuples() + tuples);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkReadTest runQueryDelta.");
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
	
	
	private void runQueryHudi(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			sqlQuery = sqlQuery.replace("<SUFFIX>", "_hudi_temp");
			String nQueryStr = sqlFilename.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			System.out.println("Processing query " + index + ": " + sqlFilename);
			this.logger.info("Processing query " + index + ": " + sqlFilename);
			queryRecord = new QueryRecord(nQuery);
			queryRecord.setStartTime(System.currentTimeMillis());
			Dataset<Row> hudiDS = this.spark.read()
					.format("org.apache.hudi")
					.option("hoodie.datasource.query.type", "snapshot")
					.load(this.extTablePrefixCreated.get() + "/store_sales_denorm_hudi/*");
			hudiDS.createOrReplaceTempView("store_sales_denorm_hudi_temp");		
			Dataset<Row> resultDS = this.spark.sql(sqlQuery);
			String resFileName = this.workDir + "/" + this.resultsDir + "/readresults/" +
					this.experimentName + "/" + this.instance +
					"/" + sqlFilename + ".txt";
			int tuples = this.saveResults(resFileName, resultDS, false);
			queryRecord.setTuples(queryRecord.getTuples() + tuples);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkReadTest runQueryHudi.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private void dropTable(String dropStmt) {
		try {
			this.spark.sql(dropStmt);
		}
		catch(Exception ignored) {
			//Do nothing.
		}
	}

	
	private int saveResults(String resFileName, Dataset<Row> dataset, boolean append) {
		int tuples = 0;
		try {
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName, append);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			List<String> list = dataset.map(row -> row.mkString(" | "), Encoders.STRING()).collectAsList();
			for(String s: list)
				printWriter.println(s);
			printWriter.close();
			tuples = list.size();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in saving results: " + resFileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return tuples;
	}
	
	
	private void countRowsQuery(String tableName) {
		try {
			String sqlCount = "select count(*) from " + tableName;
			System.out.print("Running count query on " + tableName + ": ");
			this.logger.info("Running count query on " + tableName + ": ");
			Dataset<Row> countDataset = this.spark.sql(sqlCount);
			List<String> list = countDataset.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
			for(String s: list) {
				System.out.println(s);
				this.logger.info("Count result: " + s);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkReadTest closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


