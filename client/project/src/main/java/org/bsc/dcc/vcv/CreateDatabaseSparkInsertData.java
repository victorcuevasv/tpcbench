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


public class CreateDatabaseSparkInsertData {

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
	private final Map<String, String> skipKeys;
	//private final double[] fractions = {0.1, 1.0, 10.0};
	private final double[] fractions = {10.0};
	//private final String[] insertSuffix = {"pointone", "one", "ten"};
	private final String[] insertSuffix = {"ten"};
	private final int dateskThreshold;
	private final boolean useClusterBy;
	private final Map<String, String> primaryKeys;
	
	
	public CreateDatabaseSparkInsertData(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkInsertData constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = "insertdata";
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.createTableDir = "QueriesDenorm";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		this.format = commandLine.getOptionValue("table-format");
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
		this.skipKeys = new SkipKeys().getMap();
		String dateskThresholdStr = commandLine.getOptionValue("datesk-gt-threshold", "-1");
		this.dateskThreshold = Integer.parseInt(dateskThresholdStr);
		String useClusterByStr = commandLine.getOptionValue("use-cluster-by", "false");
		this.useClusterBy = Boolean.parseBoolean(useClusterByStr);
		this.primaryKeys = new HudiPrimaryKeys().getMap();
	}
	

	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkInsertData application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkInsertData main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkInsertData(commandLine);
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
			if( this.format.equals("parquet") ) {
				for(int j = 0; j < this.fractions.length; j++) {
					createInsertTableParquet(fileName, i, j);
				}
				i++;
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
			this.logger.error("Error in CreateDatabaseSparkInsertData useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void createInsertTableParquet(String sqlCreateFilename, int index, int fractionIndex) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String denormTableName = tableName + "_denorm";
			String insertTableName = denormTableName + "_insert_" + this.insertSuffix[fractionIndex];
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			this.dropTable("drop table if exists " + insertTableName);
			String parquetSqlCreate = this.parquetCreateTableMain(tableName, denormTableName,
					insertTableName, this.extTablePrefixCreated, fractionIndex);
			saveCreateTableFile("insertcreate", insertTableName, parquetSqlCreate);
			if( this.doCount )
				countRowsQuery(denormTableName);
			//queryRecord = new QueryRecord(index);
			queryRecord = new QueryRecord(fractionIndex);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(parquetSqlCreate);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(insertTableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkInsertData createTable.");
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

	
	private String parquetCreateTableMain(String tableName, String denormTableName, 
			String insertTableName, Optional<String> extTablePrefixCreated,
			int fractionIndex) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
		String skipAtt = this.skipKeys.get(tableName);
		StringBuilder builder = new StringBuilder("CREATE TABLE " + insertTableName + "\n");
		builder.append("USING PARQUET\n");
		builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + insertTableName + "' \n");
		builder.append("AS\n");
		builder.append("( SELECT * FROM " + denormTableName + "\n");
		builder.append("WHERE MOD(" + partKey + ", " + SkipMods.firstMod + ") = 0 \n");
		if( this.dateskThreshold != -1 )
			builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
		builder.append("AND MOD(" + skipAtt + ", " + SkipMods.secondMod + ") = 0 ) \n");
		String updateExpr = this.createUpdatesExpression(denormTableName, partKey, skipAtt);
		builder.append("UNION ALL\n");
		builder.append(updateExpr);
		if( this.useClusterBy )
			builder.append("CLUSTER BY " + this.primaryKeys.get(tableName) + "\n");
		else
			builder.append("DISTRIBUTE BY " + partKey + "\n");
		return builder.toString();
	}

	
	private String createUpdatesExpression(String denormTableName, String partKey, String skipAtt) {
		String expr = null;
		try {
			Dataset<Row> dataset = this.spark.sql("DESCRIBE " + denormTableName);
			String columnsStr = getColumnNames(dataset);
			String columnsStrUpd = columnsStr.replace("s_quantity", "s_quantity + 1");
			StringBuilder builder = new StringBuilder();
			builder.append(
					"( SELECT \n" +
					 columnsStrUpd + "\n" +
					 "FROM " + denormTableName + "\n" +
					 "WHERE MOD(" + partKey + ", " + UpdateMods.firstMod + ") = 1 \n"
					 );
					 if( this.dateskThreshold != -1 )
							builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
			builder.append("AND MOD(" + skipAtt + ", " + UpdateMods.secondMod + ") = 0 ) \n");
			expr = builder.toString();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkInsertData createUpdatesExpression.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return expr;
	}
	
	
	private String getColumnNames(Dataset<Row> dataset) {
		String retVal = null;
		try {
			List<String> list = dataset.map(row -> row.getString(0), Encoders.STRING()).collectAsList();
			String columnsStr = list.stream()
					.map(x -> x)
					.filter(s -> ! s.startsWith("#"))
					.distinct()
					.collect(Collectors.joining(", \n"));
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
			this.logger.error("Error in CreateDatabaseSparkInsertData closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


