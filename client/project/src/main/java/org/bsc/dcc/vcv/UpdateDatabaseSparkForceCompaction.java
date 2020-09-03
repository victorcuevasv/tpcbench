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


public class UpdateDatabaseSparkForceCompaction {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final JarCreateTableReaderAsZipFile createTableReader;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String createTableDir;
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
	private final HudiUtil hudiUtil;
	private final String hudiFileSize;
	private final boolean hudiUseMergeOnRead;
	private final boolean defaultCompaction;
	private final int compactInstance;
	
	public UpdateDatabaseSparkForceCompaction(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		String compactInstanceStr = commandLine.getOptionValue("compact-instance", "0");
		this.compactInstance = Integer.parseInt(compactInstanceStr);
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		//this.test = commandLine.getOptionValue("tpcds-test", "loadupdate");
		this.test = "compacttest" + this.compactInstance;
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		if( this.system.equals("sparkdatabricks") )
			this.createTableDir = "DatabricksDeltaGdpr";
		else
			this.createTableDir = "QueriesDenorm";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		//this.format = commandLine.getOptionValue("table-format");
		if( this.system.equalsIgnoreCase("sparkdatabricks") )
			this.format = "delta";
		else
			this.format = "hudi";
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.denormSingleOrAll = commandLine.getOptionValue("denorm-all-or-file", "all");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.primaryKeys = new HudiPrimaryKeys().getMap();
		this.customerSK = commandLine.getOptionValue("gdpr-customer-sk");
		this.hudiFileSize = commandLine.getOptionValue("hudi-file-max-size", "1073741824");
		String hudiUseMergeOnReadStr = commandLine.getOptionValue("hudi-merge-on-read", "true");
		this.hudiUseMergeOnRead = Boolean.parseBoolean(hudiUseMergeOnReadStr);
		this.defaultCompaction = true;
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
	}
	

	public static void main(String[] args) throws SQLException {
		UpdateDatabaseSparkForceCompaction application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseSparkForceCompaction main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseSparkForceCompaction(commandLine);
		application.compactTables();
	}
	
	
	private void compactTables() {
		// Process each .sql create table file found in the jar file.
		this.useDatabase(this.dbName);
		this.recorder.header();
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQuery = this.createTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			if( this.format.equals("delta") ) {
				;
			}
			else if( this.format.equals("hudi") ) {
				forceCompactTableHudi(fileName, i);
			}
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
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void forceCompactTableHudi(String sqlFilename, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			String denormTableName = tableName + "_denorm";
			String denormHudiTableName = tableName + "_denorm_hudi";
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String selectSql = "SELECT * FROM " + denormTableName + " WHERE false LIMIT 1";
			String primaryKey = this.primaryKeys.get(tableName);
			String precombineKey = this.precombineKeys.get(tableName);
			Map<String, String> hudiOptions = null;
			if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
				String partitionKey = 
						Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
				hudiOptions = this.hudiUtil.createHudiOptions(tableName + "_denorm_hudi", 
						primaryKey, precombineKey, partitionKey, true);
			}
			else {
				hudiOptions = this.hudiUtil.createHudiOptions(tableName + "_denorm_hudi", 
						primaryKey, precombineKey, null, false);
			}
			//this.hudiUtil.saveHudiOptions("compacthudi", tableName, hudiOptions);
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					countRowsQuery(denormHudiTableName + "_ro");
				else
					countRowsQuery(denormHudiTableName);
			}
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(selectSql)
			.write()
			.format("org.apache.hudi")
			.option("hoodie.datasource.write.operation", "upsert")
			.options(hudiOptions)
			.mode(SaveMode.Append)
			.save(this.extTablePrefixCreated.get() + "/" + tableName + "_denorm_hudi" + "/");
			queryRecord.setSuccessful(true);
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					countRowsQuery(denormHudiTableName + "_ro");
				else
					countRowsQuery(denormHudiTableName);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction forceCompactTableHudi.");
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
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}

