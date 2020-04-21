package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.stream.Collectors;
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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseSparkCLI {

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
	private final CommandLine commandLine;
	
	
	public CreateDatabaseSparkCLI(String[] args) throws Exception {
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = this.commandLine.getOptionValue("main-work-dir");
		this.dbName = this.commandLine.getOptionValue("schema-name");
		this.resultsDir = this.commandLine.getOptionValue("results-dir");
		this.experimentName = this.commandLine.getOptionValue("experiment-name");
		this.system = this.commandLine.getOptionValue("system-name");
		this.test = this.commandLine.getOptionValue("tpcds-test", "load");
		String instanceStr = this.commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.rawDataDir = this.commandLine.getOptionValue("raw-data-dir", "UNUSED");
		this.createTableDir = this.commandLine.getOptionValue("create-table-dir", "tables");
		this.suffix = this.commandLine.getOptionValue("text-file-suffix", "_ext");
		this.extTablePrefixRaw = Optional.ofNullable(this.commandLine.getOptionValue("ext-raw-data-location"));
		this.extTablePrefixCreated = Optional.ofNullable(this.commandLine.getOptionValue("ext-tables-location"));
		this.format = this.commandLine.getOptionValue("table-format");
		String doCountStr = this.commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = this.commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.jarFile = this.commandLine.getOptionValue("jar-file");
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
	}


	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkCLI application = null;
		try {
			application = new CreateDatabaseSparkCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
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
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, true, suffix, false);
			String extSqlCreate = externalCreateTable(incExtSqlCreate, tableName, rawDataDir, extTablePrefixRaw);
			saveCreateTableFile("textfile", tableName, extSqlCreate);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.dropTable("drop table if exists " + tableName + suffix);
			this.spark.sql(extSqlCreate);
			if( doCount )
				countRowsQuery(tableName + suffix);
			//String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", true);
			String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
			String intSqlCreate = internalCreateTable(incIntSqlCreate, tableName, extTablePrefixCreated,
					format);
			saveCreateTableFile("parquet", tableName, intSqlCreate);
			this.dropTable("drop table if exists " + tableName);
			this.spark.sql(intSqlCreate);
			
			String insertSql = "INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix;
			if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName)) {
				List<String> columns = extractColumnNames(incIntSqlCreate); 
				insertSql = createPartitionInsertStmt(tableName, columns, suffix, format);
			}
			saveCreateTableFile("insert", tableName, insertSql);
			this.spark.sql(insertSql);
			
			/*
			String selectSql = "SELECT * FROM " + tableName + suffix;
			if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
				List<String> columns = extractColumnNames(incIntSqlCreate); 
				selectSql = createPartitionSelectStmt(tableName, columns, suffix, format);
			}
			saveCreateTableFile("select", tableName, selectSql);
			this.spark.sql(selectSql).coalesce(64).write().mode("overwrite").insertInto(tableName);
			*/
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(tableName);
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

	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	private String internalCreateTable(String incompleteSqlCreate, String tableName,
			Optional<String> extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( extTablePrefixCreated.isPresent() ) {
			if( format.equalsIgnoreCase("DELTA") )
				builder.append("USING DELTA \n");
			else
				//builder.append("USING org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat \n");
				builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			if( this.partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n" );
			}
			builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		}
		else {
			builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			if( this.partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					//Use for Hive format.
					//builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + " integer) \n" );
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n");
			}
			//Use for Hive format.
			//builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
		}
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
	
	
	private List<String> extractColumnNames(String sqlStr) {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
				continue;
				if( line.trim().startsWith("create") || line.trim().startsWith("(") || line.trim().startsWith(")") )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				list.add(tokenizer.nextToken());
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return list;
	}
	
	
	private String createPartitionInsertStmt(String tableName, List<String> columns, String suffix,
			String format) {
		StringBuilder builder = new StringBuilder();
		//builder.append("INSERT OVERWRITE TABLE " + tableName + " PARTITION (" +
		//		Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + ") SELECT \n");
		builder.append("INSERT OVERWRITE TABLE " + tableName + " SELECT \n");
		if( ! format.equalsIgnoreCase("delta") ) {
			for(String column : columns) {
				if ( column.equalsIgnoreCase(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] ))
					continue;
				else
					builder.append(column + ", \n");
			}
			builder.append(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + " \n");
		}
		else
			builder.append("* \n");
		builder.append("FROM " + tableName + suffix + "\n");
		builder.append("DISTRIBUTE BY " + Partitioning.distKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + "\n");
		return builder.toString();
	}
	
	
	private String createPartitionSelectStmt(String tableName, List<String> columns, String suffix,
			String format) {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT \n");
		if( ! format.equalsIgnoreCase("delta") ) {
			for(String column : columns) {
				if ( column.equalsIgnoreCase(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] ))
					continue;
				else
					builder.append(column + ", \n");
			}
			builder.append(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + " \n");
		}
		else
			builder.append("* \n");
		builder.append("FROM " + tableName + suffix + "\n");
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

