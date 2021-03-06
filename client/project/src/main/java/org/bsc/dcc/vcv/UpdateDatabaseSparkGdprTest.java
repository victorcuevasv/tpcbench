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


public class UpdateDatabaseSparkGdprTest {

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
	private final boolean icebergCompact;
	
	public UpdateDatabaseSparkGdprTest(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkGdprTest constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		//this.test = commandLine.getOptionValue("tpcds-test", "loadupdate");
		this.test = "gdprtest";
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.format = commandLine.getOptionValue("table-format");
		if( this.system.equalsIgnoreCase("sparkdatabricks") )
			this.format = "delta";
		else
			this.format = commandLine.getOptionValue("update-table-format", "hudi");
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		//if( this.system.equals("sparkdatabricks") )
		if( this.format.equals("delta") )
			this.createTableDir = "DatabricksDeltaGdpr";
		else if( this.format.equals("iceberg"))
			this.createTableDir = "EMRIcebergGdpr";
		else
			this.createTableDir = "EMRHudiGdpr";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
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
		String defaultCompactionStr = commandLine.getOptionValue("hudi-mor-default-compaction", "true");
		this.defaultCompaction = Boolean.parseBoolean(defaultCompactionStr);
		String icebergCompactStr = commandLine.getOptionValue("iceberg-compact", "false");
		this.icebergCompact = Boolean.parseBoolean(icebergCompactStr);
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
	}
	

	public static void main(String[] args) throws SQLException {
		UpdateDatabaseSparkGdprTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseSparkGdprTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseSparkGdprTest(commandLine);
		application.createTables();
	}
	
	
	private void createTables() {
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
			if( this.format.equals("delta") || this.format.equals("iceberg")) {
				i = deleteFromTableDeltaIceberg(fileName, sqlQuery, i);
			}
			else if( this.format.equals("hudi") ) {
				i = deleteFromTableHudi(fileName, sqlQuery, i);
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
			this.logger.error("Error in UpdateDatabaseSparkGdprTest useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private int deleteFromTableDeltaIceberg(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		QueryRecord queryRecordRewrite = null;
		try {
			sqlQuery = sqlQuery.replace("<CUSTOMER_SK>", this.customerSK);
			sqlQuery = sqlQuery.replace("<FORMAT>", this.format);
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_" + this.format);
			saveCreateTableFile(this.format + "gdpr", tableName, sqlQuery);
			queryRecord = new QueryRecord(index);
			if( this.format.equals("iceberg") && this.icebergCompact ) {
				index += 1;
				queryRecordRewrite = new QueryRecord(index);
			}
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(sqlQuery);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.format.equals("iceberg") && this.icebergCompact ) {
				queryRecordRewrite.setStartTime(System.currentTimeMillis());
				IcebergUtil icebergUtil = new IcebergUtil();
				long fileSize = Long.parseLong(this.hudiFileSize);
				icebergUtil.rewriteData(this.spark, this.dbName, tableName + "_denorm_" + this.format,
						fileSize);
				queryRecordRewrite.setSuccessful(true);
				queryRecordRewrite.setEndTime(System.currentTimeMillis());
			}
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_" + this.format);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkGdprTest deleteFromTableDeltaIceberg.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
			if( this.format.equals("iceberg") && queryRecordRewrite != null ) {
				this.recorder.record(queryRecordRewrite);
			}
		}
		return index + 1;
	}
	
	
	private int deleteFromTableHudi(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord1 = null;
		QueryRecord queryRecord2 = null;
		try {
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					countRowsQuery(tableName + "_denorm_hudi_rt");
				else
					countRowsQuery(tableName + "_denorm_hudi");
			}
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
			this.hudiUtil.saveHudiOptions("hudigdpr", tableName, hudiOptions);
			sqlQuery = sqlQuery.replace("<CUSTOMER_SK>", this.customerSK);
			//For Merge on Read, use the _rt view.
			if( this.hudiUseMergeOnRead )
				//sqlQuery = sqlQuery.replace("<SUFFIX>", "_rt");
				sqlQuery = sqlQuery.replace("<SUFFIX>", "_temp");
			else
				//sqlQuery = sqlQuery.replace("<SUFFIX>", "");
				sqlQuery = sqlQuery.replace("<SUFFIX>", "_temp");
			queryRecord1 = new QueryRecord(index);
			queryRecord1.setStartTime(System.currentTimeMillis());
			Dataset<Row> hudiDS = this.spark.read()
					.format("org.apache.hudi")
					.option("hoodie.datasource.query.type", "snapshot")
					.load(this.extTablePrefixCreated.get() + "/" + tableName + "_denorm_hudi" + "/*");
			hudiDS.createOrReplaceTempView(tableName + "_denorm_hudi_temp");
			//Disable the vectorized reader to avoid an array index out of bounds exception at 1 TB
			this.spark.sql("SET spark.sql.parquet.enableVectorizedReader = false");
			Dataset<Row> resultDS = this.spark.sql(sqlQuery);
			queryRecord1.setEndTime(System.currentTimeMillis());
			String resFileName = this.workDir + "/" + this.resultsDir + "/gdprdata/" +
					this.experimentName + "/" + this.instance +
					"/" + tableName + ".txt";
			int tuples = this.saveResults(resFileName, resultDS, false);
			queryRecord1.setTuples(queryRecord1.getTuples() + tuples);
			queryRecord1.setSuccessful(true);
			queryRecord2 = new QueryRecord(index + 1);
			queryRecord2.setStartTime(System.currentTimeMillis());
			resultDS.write()
				.format("org.apache.hudi")
				.option("hoodie.datasource.write.operation", "upsert")
				.option("hoodie.datasource.write.payload.class", 
						"org.apache.hudi.common.model.EmptyHoodieRecordPayload")
				.options(hudiOptions)
				.mode(SaveMode.Append)
				.save(this.extTablePrefixCreated.get() + "/" + tableName + "_denorm_hudi" + "/");
			//Enable the vectorized reader disabled above to avoid an array index out of bounds 
			//exception at 1 TB
			this.spark.sql("SET spark.sql.parquet.enableVectorizedReader = true");
			queryRecord2.setSuccessful(true);
			queryRecord2.setEndTime(System.currentTimeMillis());
			saveCreateTableFile("hudigdpr", tableName, sqlQuery);
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					countRowsQuery(tableName + "_denorm_hudi_rt");
				else
					countRowsQuery(tableName + "_denorm_hudi");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkGdprTest deleteFromTableHudi.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord1 != null ) {
				this.recorder.record(queryRecord1);
			}
			if( queryRecord2 != null ) {
				this.recorder.record(queryRecord2);
			}
		}
		return index + 2;
	}
	
	
	private void dropTable(String dropStmt) {
		try {
			this.spark.sql(dropStmt);
		}
		catch(Exception ignored) {
			//Do nothing.
		}
	}

	
	public void saveCreateTableFile(String suffix, String tableName, String sqlCreate) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".sql";
			File temp = new File(createTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(createTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(sqlCreate);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
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
			this.logger.error("Error in UpdateDatabaseSparkGdprTest closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


