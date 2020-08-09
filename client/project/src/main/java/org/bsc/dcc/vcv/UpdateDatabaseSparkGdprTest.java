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
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		if( this.system.equals("sparkdatabricks") )
			this.createTableDir = "DatabricksDeltaGdpr";
		else
			this.createTableDir = "EMRHudiGdpr";
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
			if( this.format.equals("delta") )
				deleteFromTableDelta(fileName, sqlQuery, i);
			else if( this.format.equals("hudi") )
				deleteFromTableHudi(fileName, sqlQuery, i);
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
			this.logger.error("Error in UpdateDatabaseSparkGdprTest useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void deleteFromTableDelta(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			sqlQuery = sqlQuery.replace("<CUSTOMER_SK>", this.customerSK);
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_delta");
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(sqlQuery);
			queryRecord.setSuccessful(true);
			saveCreateTableFile("deltagdpr", tableName, sqlQuery);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_delta");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkGdprTest deleteFromTableDelta.");
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
	
	
	private void deleteFromTableHudi(String sqlFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_hudi_ro");
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			String primaryKey = this.primaryKeys.get(tableName);
			String precombineKey = this.precombineKeys.get(tableName);
			Map<String, String> hudiOptions = null;
			if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
				String partitionKey = 
						Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
				hudiOptions = createHudiOptions(tableName + "_denorm_hudi", 
						primaryKey, precombineKey, partitionKey, true);
			}
			else {
				hudiOptions = createHudiOptions(tableName + "_denorm_hudi", 
						primaryKey, precombineKey, null, false);
			}
			this.saveHudiOptions("hudigdpr", tableName, hudiOptions);
			sqlQuery = sqlQuery.replace("<CUSTOMER_SK>", this.customerSK);
			this.spark.sql(sqlQuery)
				.write()
				.format("org.apache.hudi")
				.option("hoodie.datasource.write.operation", "upsert")
				.option("hoodie.datasource.write.payload.class", 
						"org.apache.hudi.common.model.EmptyHoodieRecordPayload")
				.options(hudiOptions)
				.mode(SaveMode.Append)
				.save(this.extTablePrefixCreated.get() + "/" + tableName + "_denorm_hudi" + "/");
			queryRecord.setSuccessful(true);
			saveCreateTableFile("hudigdpr", tableName, sqlQuery);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_hudi_ro");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkGdprTest deleteFromTableHudi.");
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
	
	
	private Map<String, String> createHudiOptions(String tableName, String primaryKey,
			String precombineKey, String partitionKey, boolean usePartitioning) {
		Map<String, String> map = new HashMap<String, String>();
		//Use only simple keys.
		//StringTokenizer tokenizer = new StringTokenizer(primaryKey, ",");
		//primaryKey = tokenizer.nextToken();
		map.put("hoodie.datasource.hive_sync.database", this.dbName);
		map.put("hoodie.datasource.write.precombine.field", precombineKey);
		map.put("hoodie.datasource.hive_sync.table", tableName);
		map.put("hoodie.datasource.hive_sync.enable", "true");
		map.put("hoodie.datasource.write.recordkey.field", primaryKey);
		map.put("hoodie.table.name", tableName);
		//map.put("hoodie.datasource.write.storage.type", "COPY_ON_WRITE");
		map.put("hoodie.datasource.write.storage.type", "MERGE_ON_READ");
		map.put("hoodie.datasource.write.hive_style_partitioning", "true");
		map.put("hoodie.parquet.max.file.size", String.valueOf(1024 * 1024 * 1024));
		map.put("hoodie.parquet.compression.codec", "snappy");
		if( usePartitioning ) {
			map.put("hoodie.datasource.hive_sync.partition_extractor_class", 
					"org.apache.hudi.hive.MultiPartKeysValueExtractor");
			map.put("hoodie.datasource.hive_sync.partition_fields", partitionKey);
			map.put("hoodie.datasource.write.partitionpath.field", partitionKey);
			map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
			//map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
		}
		else {
			map.put("hoodie.datasource.hive_sync.partition_extractor_class", 
					"org.apache.hudi.hive.NonPartitionedExtractor");
			map.put("hoodie.datasource.hive_sync.partition_fields", "");
			map.put("hoodie.datasource.write.partitionpath.field", "");
			map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");   
		}
		return map;
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
	
	
	private void saveHudiOptions(String suffix, String tableName, Map<String, String> map) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".txt";
			StringBuilder builder = new StringBuilder();
			for (Map.Entry<String, String> entry : map.entrySet()) {
			    builder.append(entry.getKey() + "=" + entry.getValue().toString() + "\n");
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

	
	private void countRowsQuery(String tableName) {
		try {
			String sqlCount = "select count(*) from " + tableName;
			System.out.print("Running count query on " + tableName + ": ");
			Dataset<Row> countDataset = this.spark.sql(sqlCount);
			List<String> list = countDataset.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
			for(String s: list)
				System.out.println(s);
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


