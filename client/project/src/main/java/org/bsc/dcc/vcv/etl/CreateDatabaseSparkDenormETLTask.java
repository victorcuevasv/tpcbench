package org.bsc.dcc.vcv.etl;

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
import org.bsc.dcc.vcv.AnalyticsRecorder;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.FilterKeys;
import org.bsc.dcc.vcv.SkipKeys;
import org.bsc.dcc.vcv.FilterValues;
import org.bsc.dcc.vcv.HudiPrecombineKeys;
import org.bsc.dcc.vcv.HudiPrimaryKeys;
import org.bsc.dcc.vcv.HudiUtil;
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public abstract class CreateDatabaseSparkDenormETLTask {

	protected static final Logger logger = LogManager.getLogger("AllLog");
	protected SparkSession spark;
	protected final JarCreateTableReaderAsZipFile createTableReader;
	protected final AnalyticsRecorder recorder;
	protected final String workDir;
	protected final String dbName;
	protected final String resultsDir;
	protected final String experimentName;
	protected final String system;
	protected final String test;
	protected final int instance;
	protected final String createTableDir;
	protected final Optional<String> extTablePrefixCreated;
	protected final String format;
	protected final boolean doCount;
	protected final boolean partition;
	protected final String jarFile;
	protected final String createSingleOrAll;
	protected final String denormSingleOrAll;
	protected final boolean partitionIgnoreNulls;
	protected final Map<String, String> precombineKeys;
	protected final Map<String, String> primaryKeys;
	protected final Map<String, String> filterKeys;
	protected final Map<String, String> filterValues;
	protected final Map<String, String> skipKeys;
	protected final boolean partitionWithDistrubuteBy;
	protected final boolean denormWithFilter;
	protected final HudiUtil hudiUtil;
	protected final String hudiFileSize;
	protected final boolean hudiUseMergeOnRead;
	protected final boolean defaultCompaction;
	protected final int dateskThreshold;
	
	
	public CreateDatabaseSparkDenormETLTask(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenorm constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "loaddenorm");
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
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.primaryKeys = new HudiPrimaryKeys().getMap();
		this.filterKeys = new FilterKeys().getMap();
		this.skipKeys = new SkipKeys().getMap();
		this.filterValues = new FilterValues().getMap();
		String partitionWithDistrubuteByStr = commandLine.getOptionValue(
				"partition-with-distribute-by", "false");
		this.partitionWithDistrubuteBy = Boolean.parseBoolean(partitionWithDistrubuteByStr);
		String denormWithFilterStr = commandLine.getOptionValue(
				"denorm-with-filter", "true");
		this.denormWithFilter = Boolean.parseBoolean(denormWithFilterStr);
		this.hudiFileSize = commandLine.getOptionValue("hudi-file-max-size", "1073741824");
		String hudiUseMergeOnReadStr = commandLine.getOptionValue("hudi-merge-on-read", "true");
		this.hudiUseMergeOnRead = Boolean.parseBoolean(hudiUseMergeOnReadStr);
		String defaultCompactionStr = commandLine.getOptionValue("hudi-mor-default-compaction", "true");
		this.defaultCompaction = Boolean.parseBoolean(defaultCompactionStr);
		String partitionIgnoreNullsStr = commandLine.getOptionValue("partition-ignore-nulls", "false");
		this.partitionIgnoreNulls = Boolean.parseBoolean(partitionIgnoreNullsStr);
		String dateskThresholdStr = commandLine.getOptionValue("datesk-gt-threshold", "-1");
		this.dateskThreshold = Integer.parseInt(dateskThresholdStr);
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
	}
	
	
	protected abstract void doTask();

	
	protected void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenorm useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	protected void dropTable(String dropStmt) {
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
	
	
	public void saveHudiOptions(String suffix, String tableName, Map<String, String> map) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".txt";
			StringBuilder builder = new StringBuilder();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				String entryVal = entry.getValue() != null ? entry.getValue().toString() : null;
			    builder.append(entry.getKey() + "=" + entryVal + "\n");
			}
			File temp = new File(createTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(createTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(builder.toString());
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}

	
	protected void countRowsQuery(String tableName) {
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
			this.logger.error("Error in CreateDatabaseSparkDenorm closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


