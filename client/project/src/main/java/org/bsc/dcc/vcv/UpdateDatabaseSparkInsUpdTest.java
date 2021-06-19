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


public class UpdateDatabaseSparkInsUpdTest {

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
	private final Map<String, String> primaryKeys;
	private final Map<String, String> precombineKeys;
	//private final double[] fractions = {0.1, 1.0, 10.0};
	private final double[] fractions = {10.0};
	//private final String[] insUpdSuffix = {"pointone", "one", "ten"};
	private final String[] insUpdSuffix = {"ten"};
	private final HudiUtil hudiUtil;
	private final String hudiFileSize;
	private final boolean hudiUseMergeOnRead;
	private final boolean defaultCompaction;
	private final boolean icebergCompact;
	
	public UpdateDatabaseSparkInsUpdTest(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkInsUpdData constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = "insupdtest";
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.createTableDir = "QueriesDenorm";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
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
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.primaryKeys = new HudiPrimaryKeys().getMap();
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
		UpdateDatabaseSparkInsUpdTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseSparkInsUpdTesta main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseSparkInsUpdTest(commandLine);
		application.processTables();
	}
	
	
	private void processTables() {
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
			for(int j = 0; j < this.fractions.length; j++) {
				if( this.format.equals("delta") || this.format.equals("iceberg") )
					i = insUpdToDeltaIcebergTable(fileName, i, j);
				else if( this.format.equals("hudi") )
					i = insUpdToHudiTable(fileName, i, j);
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
			this.logger.error("Error in UpdateDatabaseSparkInsUpdTest useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private int insUpdToDeltaIcebergTable(String sqlCreateFilename, int index, int fractionIndex) {
		QueryRecord queryRecord = null;
		QueryRecord queryRecordRewrite = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String denormTableName = tableName + "_denorm";
			String denormDeltaIcebergTableName = tableName + "_denorm_" + this.format;
			String insUpdTableName = denormTableName + "_insert_" + this.insUpdSuffix[fractionIndex];
			System.out.println("Processing table " + index + ": " + insUpdTableName);
			this.logger.info("Processing table " + index + ": " + insUpdTableName);
			String mergeSql = this.createMergeSQL(tableName, denormDeltaIcebergTableName,
					insUpdTableName, fractionIndex);
			saveCreateTableFile("insupdmerge", insUpdTableName, mergeSql);
			if( this.doCount )
				countRowsQuery(denormDeltaIcebergTableName);
			queryRecord = new QueryRecord(index);
			if( this.format.equals("iceberg") && this.icebergCompact ) {
				index += 1;
				queryRecordRewrite = new QueryRecord(index);
			}
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(mergeSql);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.format.equals("iceberg") && this.icebergCompact ) {
				queryRecordRewrite.setStartTime(System.currentTimeMillis());
				IcebergUtil icebergUtil = new IcebergUtil();
				long fileSize = Long.parseLong(this.hudiFileSize);
				icebergUtil.rewriteData(this.spark, this.dbName, denormDeltaIcebergTableName, fileSize);
				queryRecordRewrite.setSuccessful(true);
				queryRecordRewrite.setEndTime(System.currentTimeMillis());
			}
			if( this.doCount )
				countRowsQuery(denormDeltaIcebergTableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkInsUpdTest insUpdToDeltaIcebergTable.");
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
	
	
	private int insUpdToHudiTable(String sqlCreateFilename, int index, int fractionIndex) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String denormTableName = tableName + "_denorm";
			String denormHudiTableName = tableName + "_denorm_hudi";
			String insUpdTableName = denormTableName + "_insert_" + this.insUpdSuffix[fractionIndex];
			System.out.println("Processing table " + index + ": " + insUpdTableName);
			this.logger.info("Processing table " + index + ": " + insUpdTableName);
			String selectSql = "SELECT * FROM " + insUpdTableName;
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
			this.hudiUtil.saveHudiOptions("insupdhudi", insUpdTableName, hudiOptions);
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
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					countRowsQuery(denormHudiTableName + "_ro");
				else
					countRowsQuery(denormHudiTableName);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkInsUpdTest createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
		return index + 1;
	}
	
	
	private void dropTable(String dropStmt) {
		try {
			this.spark.sql(dropStmt);
		}
		catch(Exception ignored) {
			//Do nothing.
		}
	}

	
	private String createMergeSQL(String tableName, String denormDeltaIcebergTableName, 
			String insUpdTableName, int fractionIndex) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
		String primaryKeyFull = this.primaryKeys.get(tableName);
		StringTokenizer tokenizer = new StringTokenizer(primaryKeyFull, ",");
		String primaryKey = tokenizer.nextToken().trim();
		String primaryKeyComp = null;
		if( tokenizer.hasMoreTokens() )
			primaryKeyComp = tokenizer.nextToken().trim();
		StringBuilder builder = new StringBuilder();
		builder.append("MERGE INTO " + denormDeltaIcebergTableName + " AS a \n");
		builder.append("USING " + insUpdTableName + " AS b \n");
		builder.append("ON a." + partKey + " = b." + partKey + "\n");
		builder.append("AND a." + primaryKey + " = b." + primaryKey + "\n");
		if( primaryKeyComp != null )
			builder.append("AND a." + primaryKeyComp + " = b." + primaryKeyComp + "\n");
		builder.append("WHEN MATCHED THEN UPDATE SET * \n");
		builder.append("WHEN NOT MATCHED THEN INSERT * \n");
		return builder.toString();
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
			this.logger.error("Error in UpdateDatabaseSparkInsUpdTest closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


