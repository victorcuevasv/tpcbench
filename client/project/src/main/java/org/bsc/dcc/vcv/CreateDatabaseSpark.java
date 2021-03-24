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


public class CreateDatabaseSpark {

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
	private final String rawDataDir;
	private final String createTableDir;
	private final String suffix;
	private final Optional<String> extTablePrefixRaw;
	private final Optional<String> extTablePrefixCreated;
	private final String format;
	private final boolean doCount;
	private final boolean partition;
	private final String jarFile;
	private final String createSingleOrAll;
	private final boolean partitionIgnoreNulls;
	private final Map<String, String> precombineKeys;
	private final HudiUtil hudiUtil;
	private final String hudiFileSize;
	private final boolean hudiUseMergeOnRead;
	private final boolean defaultCompaction;
	
	public CreateDatabaseSpark(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "load");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.rawDataDir = commandLine.getOptionValue("raw-data-dir", "UNUSED");
		this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.suffix = commandLine.getOptionValue("text-file-suffix", "_ext");
		this.extTablePrefixRaw = Optional.ofNullable(commandLine.getOptionValue("ext-raw-data-location"));
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		this.format = commandLine.getOptionValue("table-format");
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		String partitionIgnoreNullsStr = commandLine.getOptionValue("partition-ignore-nulls", "false");
		this.partitionIgnoreNulls = Boolean.parseBoolean(partitionIgnoreNullsStr);
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.hudiFileSize = commandLine.getOptionValue("hudi-file-max-size", "1073741824");
		String hudiUseMergeOnReadStr = commandLine.getOptionValue("hudi-merge-on-read", "true");
		this.hudiUseMergeOnRead = Boolean.parseBoolean(hudiUseMergeOnReadStr);
		String defaultCompactionStr = commandLine.getOptionValue("hudi-mor-default-compaction", "true");
		this.defaultCompaction = Boolean.parseBoolean(defaultCompactionStr);
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
	}
	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * 
	 * args[5] test name (i.e. load)
	 * args[6] experiment instance number
	 * args[7] directory for generated data raw files
	 * args[8] subdirectory within the jar that contains the create table files
	 * args[9] suffix used for intermediate table text files
	 * 
	 * args[10] prefix of external location for raw data tables (e.g. S3 bucket), null for none
	 * args[11] prefix of external location for created tables (e.g. S3 bucket), null for none
	 * args[12] format for column-storage tables (PARQUET, DELTA)
	 * args[13] whether to run queries to count the tuples generated (true/false)
	 * args[14] whether to use data partitioning for the tables (true/false)
	 * 
	 * args[15] jar file
	 * 
	 */
	public CreateDatabaseSpark(String[] args) {
		if( args.length != 16 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			this.logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		this.workDir = args[0];
		this.dbName = args[1];
		this.resultsDir = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.rawDataDir = args[7];
		this.createTableDir = args[8];
		this.suffix = args[9];
		this.extTablePrefixRaw = Optional.ofNullable(
				args[10].equalsIgnoreCase("null") ? null : args[10]);
		this.extTablePrefixCreated = Optional.ofNullable(
				args[11].equalsIgnoreCase("null") ? null : args[11]);
		this.format = args[12];
		this.doCount = Boolean.parseBoolean(args[13]);
		this.partition = Boolean.parseBoolean(args[14]);
		this.createSingleOrAll = "all";
		this.partitionIgnoreNulls = false;
		this.jarFile = args[15];
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.hudiFileSize = "1073741824";
		this.hudiUseMergeOnRead = true;
		this.defaultCompaction = true;
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}


	public static void main(String[] args) throws SQLException {
		CreateDatabaseSpark application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new CreateDatabaseSpark(args);
		else {
			CommandLine commandLine = null;
			try {
				RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
				Options options = runOptions.getOptions();
				CommandLineParser parser = new DefaultParser();
				commandLine = parser.parse(options, args);
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error("Error in CreateDatabaseSpark main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new CreateDatabaseSpark(commandLine);
		}
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
			String sqlCreate = this.createTableReader.getFile(fileName);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if( fileName.equals("dbgen_version.sql") ) {
				System.out.println("Skipping: " + fileName);
				continue;
			}
			if( ! this.createSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.createSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			createTable(fileName, sqlCreate, i);
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
			this.logger.error("Error in CreateDatabaseSpark useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be manipulated for
	// creating these tables.
	private void createTable(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, true, this.suffix, false);
			String extSqlCreate = externalCreateTable(incExtSqlCreate, tableName, 
					this.rawDataDir, this.extTablePrefixRaw);
			saveCreateTableFile("textfile", tableName, extSqlCreate);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.dropTable("drop table if exists " + tableName + this.suffix);
			this.spark.sql(extSqlCreate);
			if( this.doCount )
				countRowsQuery(tableName + this.suffix);
			if( this.format.equals("hudi") )
				createInternalTableHudi(sqlCreate, tableName);
			else
				createInternalTableSQL(sqlCreate, tableName);
			queryRecord.setSuccessful(true);
			if( this.doCount ) {
				if( this.format.equals("hudi") && this.hudiUseMergeOnRead )
					countRowsQuery(tableName + "_ro");
				else
					countRowsQuery(tableName);
			}
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
	
	
	private void createInternalTableSQL(String sqlCreate, String tableName) throws Exception {
		//String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", true);
		String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
		String intSqlCreate = CreateDatabaseSparkUtil.internalCreateTable(incIntSqlCreate, tableName, 
				this.extTablePrefixCreated, this.format, this.partition);
		//saveCreateTableFile("parquet", tableName, intSqlCreate);
		saveCreateTableFile("create" + this.format, tableName, intSqlCreate);
		this.dropTable("drop table if exists " + tableName);
		this.spark.sql(intSqlCreate);
		String insertSql = "INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix;
		if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName)) {
			List<String> columns = CreateDatabaseSparkUtil.extractColumnNames(incIntSqlCreate);
			insertSql = this.createPartitionInsertStmt(tableName, columns, this.suffix, this.format,
					this.partitionIgnoreNulls);
		}
		//saveCreateTableFile("insert", tableName, insertSql);
		saveCreateTableFile("insert" + this.format, tableName, insertSql);
		this.spark.sql(insertSql);
		/*
		String selectSql = "SELECT * FROM " + tableName + suffix;
		if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
			List<String> columns = extractColumnNames(incIntSqlCreate); 
			selectSql = CreateDatabaseSpark.createPartitionSelectStmt(tableName, columns, suffix, 
				format, this.partitionIgnoreNulls);
		}
		saveCreateTableFile("select", tableName, selectSql);
		this.spark.sql(selectSql).coalesce(64).write().mode("overwrite").insertInto(tableName);	
		 */
	}
	
	
	private void createInternalTableHudi(String sqlCreate, String tableName) throws Exception {
		String primaryKey = extractPrimaryKey(sqlCreate);
		String precombineKey = this.precombineKeys.get(tableName);
		Map<String, String> hudiOptions = null;
		if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
			String partitionKey = 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
			hudiOptions = this.hudiUtil.createHudiOptions(tableName, 
					primaryKey, precombineKey, partitionKey, true);
		}
		else {
			hudiOptions = this.hudiUtil.createHudiOptions(tableName, 
					primaryKey, precombineKey, null, false);
		}
		saveHudiOptions("hudi", tableName, hudiOptions);
		String selectSql = "SELECT * FROM " + tableName + this.suffix;
		if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) && 
				this.partitionIgnoreNulls ) {
			selectSql = selectSql + " WHERE " + 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] +
					" is not null";
		}
		this.spark.sql(selectSql).write().format("org.apache.hudi")
		  .option("hoodie.datasource.write.operation", "insert")
		  .options(hudiOptions).mode(SaveMode.Overwrite)
		  .save(this.extTablePrefixCreated.get() + "/" + tableName + "/");
	}
	
	
	private String extractPrimaryKey(String sqlCreate) {
		String primaryKeyLine = Stream.of(sqlCreate.split("\\r?\\n")).
				filter(s -> s.contains("primary key")).findAny().orElse(null);
		if( primaryKeyLine == null ) {
			System.out.println("Null value in extractPrimaryKey.");
			this.logger.error("Null value in extractPrimaryKey.");
		}
		String primaryKey = primaryKeyLine.trim();
		primaryKey = primaryKey.substring(primaryKey.indexOf('(') + 1, primaryKey.indexOf(')'));
		return primaryKey.replace(" ", "");
	}
	
	
	private void dropTable(String dropStmt) {
		try {
			this.spark.sql(dropStmt);
		}
		catch(Exception ignored) {
			//Do nothing.
		}
	}
	
	
	// Generate an incomplete SQL create statement to be completed for the texfile
	// external and
	// parquet internal tables.
	private String incompleteCreateTable(String sqlCreate, String tableName, boolean external, String suffix,
			boolean checkPartitionKey) {
		boolean hasPrimaryKey = sqlCreate.contains("primary key");
		// Remove the 'not null' statements.
		sqlCreate = sqlCreate.replace("not null", "        ");
		// Split the SQL create table statement in lines.
		String lines[] = sqlCreate.split("\\r?\\n");
		// Split the first line to insert the external keyword.
		String[] firstLine = lines[0].split("\\s+");
		// The new line should have external inserted.
		String firstLineNew = "";
		if (external)
			firstLineNew = firstLine[0] + " external " + firstLine[1] + " " + firstLine[2] + suffix + " \n";
		else
			firstLineNew = firstLine[0] + " " + firstLine[1] + " " + firstLine[2] + suffix + " \n";
		// Add all of the lines in the original SQL to the first line, except those
		// which contain the primary key statement (if any) and the closing parenthesis.
		// For the last column statement, remove the final comma.
		StringBuilder builder = new StringBuilder(firstLineNew);
		int tail = hasPrimaryKey ? 3 : 2;
		for (int i = 1; i < lines.length - tail; i++) {
			if( checkPartitionKey && this.partition && 
				Arrays.asList(Partitioning.tables).contains(tableName) &&
				lines[i].contains(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)])) {
					continue;
			}
			else
				builder.append(lines[i] + "\n");
		}
		// Change the last comma for a space (since the primary key statement was
		// removed).
		char[] commaLineArray = lines[lines.length - tail].toCharArray();
		commaLineArray[commaLineArray.length - 1] = ' ';
		builder.append(new String(commaLineArray) + "\n");
		// Close the parenthesis.
		builder.append(") \n");
		// Version 2.1 of Hive does not recognize integer, so use int instead.
		return builder.toString().replace("integer", "int    ");
	}

	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Hive.
	private String externalCreateTable(String incompleteSqlCreate, String tableName, String rawDataDir,
			Optional<String> extTablePrefixRaw) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' \n");
		builder.append("STORED AS TEXTFILE \n");
		if( extTablePrefixRaw.isPresent() )
			builder.append("LOCATION '" + extTablePrefixRaw.get() + "/" + tableName + "' \n");
		else
			builder.append("LOCATION '" + rawDataDir + "/" + tableName + "' \n");
		return builder.toString();
	}

	
	public void saveCreateTableFile(String suffix, String tableName, String sqlCreate) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + this.createTableDir +
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
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + this.createTableDir +
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

	
	public String readFileContents(String filename) {
		BufferedReader inBR = null;
		String retVal = null;
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line = null;
			StringBuilder builder = new StringBuilder();
			while ((line = inBR.readLine()) != null) {
				builder.append(line + "\n");
			}
			retVal = builder.toString();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return retVal;
	}
	
	
	private String createPartitionInsertStmt(String tableName, List<String> columns, String suffix,
			String format, boolean partitionIgnoreNulls) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT OVERWRITE TABLE " + tableName + "\n");
		String selectStmt = CreateDatabaseSparkUtil.createPartitionSelectStmt(tableName, columns, suffix, 
				format, partitionIgnoreNulls);
		builder.append(selectStmt);
		return builder.toString();
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


