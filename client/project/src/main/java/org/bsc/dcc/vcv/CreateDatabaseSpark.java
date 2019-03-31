package org.bsc.dcc.vcv;

import java.util.List;
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

public class CreateDatabaseSpark {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private JarCreateTableReaderAsZipFile createTableReader;
	private AnalyticsRecorder recorder;

	public CreateDatabaseSpark(String jarFile, String subDir, String system) {
		try {
			this.createTableReader = new JarCreateTableReaderAsZipFile(jarFile, subDir);
			this.spark = SparkSession.builder().appName("Java Create Database Spark")
				.enableHiveSupport()
				.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.recorder = new AnalyticsRecorder("load", system);
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] subdirectory within main work directory for the create table files (read from the jar)
	 * args[1] suffix used for intermediate table text files
	 * args[2] directory for generated data raw files
	 * args[3] hostname of the server
	 * args[4] system running the data loading queries
	 * args[5] whether to run queries to count the tuples generated
	 * args[6] subdirectory within the jar that contains the create table files
	 * args[7] jar file
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length != 8 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(0);
		}
		CreateDatabaseSpark prog = new CreateDatabaseSpark(args[7], args[6], args[4]);
		boolean doCount = Boolean.parseBoolean(args[5]);
		prog.createTables(args[0], args[1], args[2], doCount);
		prog.closeConnection();
	}
	
	private void createTables(String workDir, String suffix, String genDataDir, boolean doCount) {
		// Process each .sql create table file found in the jar file.
		int i = 0;
		for (final String fileName : this.createTableReader.getFiles()) {
			String sqlCreate = this.createTableReader.getFile(fileName);
			createTable(workDir, fileName, sqlCreate, suffix, genDataDir, doCount, i);
			i++;
		}
	}

	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be manipulated for
	// creating these tables.
	private void createTable(String workDir, String sqlCreateFilename, String sqlCreate, String suffix, 
			String genDataDir, boolean doCount, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, true, suffix);
			String extSqlCreate = externalCreateTable(incExtSqlCreate, tableName, genDataDir);
			saveCreateTableFile(workDir, "textfile", tableName, extSqlCreate);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if (tableName.equals("dbgen_version")) {
				System.out.println("Skipping: " + tableName);
				return;
			}
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql("drop table if exists " + tableName + suffix);
			this.spark.sql(extSqlCreate);
			if( doCount )
				countRowsQuery(tableName + suffix);
			String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "");
			String intSqlCreate = internalCreateTable(incIntSqlCreate, tableName);
			saveCreateTableFile(workDir, "parquet", tableName, intSqlCreate);
			this.spark.sql("drop table if exists " + tableName);
			this.spark.sql(intSqlCreate);
			this.spark.sql("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix);
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(tableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}

	// Generate an incomplete SQL create statement to be completed for the texfile
	// external and
	// parquet internal tables.
	private String incompleteCreateTable(String sqlCreate, String tableName, boolean external, String suffix) {
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
	private String externalCreateTable(String incompleteSqlCreate, String tableName, String genDataDir) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' \n");
		builder.append("STORED AS TEXTFILE \n");
		builder.append("LOCATION '" + genDataDir + "/" + tableName + "' \n");
		return builder.toString();
	}

	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	private String internalCreateTable(String incompleteSqlCreate, String tableName) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
		return builder.toString();
	}

	public void saveCreateTableFile(String workDir, String suffix, String tableName, String sqlCreate) {
		try {
			File temp = new File(workDir + suffix + "/" + tableName + ".sql");
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(workDir + suffix + "/" + tableName + ".sql");
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


