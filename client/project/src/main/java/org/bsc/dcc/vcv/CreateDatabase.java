package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.List;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class CreateDatabase {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorder recorder;
	private String workDir;
	private String dbName;
	private String folderName;
	private String experimentName;
	private String system;
	private String test;
	private int instance;
	private String genDataDir;
	private String subDir;
	private String suffix;
	private String extTablePrefixRaw;
	private String extTablePrefixCreated;
	private String format;
	private boolean doCount;
	private String hostname;
	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * args[5] test name (i.e. load)
	 * args[6] experiment instance number
	 * args[7] directory for generated data raw files
	 * args[8] subdirectory within the jar that contains the create table files
	 * args[9] suffix used for intermediate table text files
	 * args[10] prefix of external location for raw data tables (e.g. S3 bucket), null for none
	 * args[11] prefix of external location for created tables (e.g. S3 bucket), null for none
	 * args[12] format for column-storage tables (PARQUET, DELTA)
	 * args[13] whether to run queries to count the tuples generated (true/false)
	 * args[14] hostname of the server
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CreateDatabase(String[] args) {
		try {
			this.workDir = args[0];
			this.dbName = args[1];
			this.folderName = args[2];
			this.experimentName = args[3];
			this.system = args[4];
			this.test = args[5];
			this.instance = Integer.parseInt(args[6]);
			this.genDataDir = args[7];
			this.subDir = args[8];
			this.suffix = args[9];
			this.extTablePrefixRaw = args[10].equalsIgnoreCase("null") ? null : args[10];
			this.extTablePrefixCreated = args[11].equalsIgnoreCase("null") ? null : args[11];
			this.format = args[12];
			this.doCount = Boolean.parseBoolean(args[13]);
			this.hostname = args[14];
			if( this.system.equals("hive") ) {
				Class.forName(driverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" + this.hostname + 
					":10000/" + dbName, "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8080/hive/" + this.dbName, "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hadoop", "");
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + this.system);
			}
			this.recorder = new AnalyticsRecorder(this.workDir, this.folderName, this.experimentName,
					this.system, this.test, this.instance);
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
	}

	
	public static void main(String[] args) throws SQLException {
		if( args.length != 15 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		CreateDatabase prog = new CreateDatabase(args);
		prog.createTables();
		prog.closeConnection();
	}
	
	
	private void createTables() {
		File directory = new File(this.workDir + "/" + this.subDir);
		this.recorder.header();
		// Process each .sql create table file found in the directory.
		File[] filesArray = directory.listFiles();
		List<File> filesList = Arrays.stream(filesArray).sorted().collect(Collectors.toList());
		int i = 1;
		for (final File fileEntry : filesList) {
			if (!fileEntry.isDirectory()) {
				this.createTable(fileEntry, i);
				i++;
			}
		}
	}

	
	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be modified for
	// creating these tables.
	private void createTable(File tableSQLfile, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = tableSQLfile.getName().substring(0, tableSQLfile.getName().indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String sqlCreate = readFileContents(tableSQLfile.getAbsolutePath());
			//Hive and Spark use the statement 'create external table ...' for raw data tables
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, 
					! this.recorder.system.startsWith("presto"), suffix);
			String extSqlCreate = null;
			if( this.recorder.system.equals("hive") || this.recorder.system.equals("spark"))
				extSqlCreate = externalCreateTableHive(incExtSqlCreate, tableName, genDataDir, 
						extTablePrefixRaw);
			else if( this.recorder.system.startsWith("presto") )
				extSqlCreate = externalCreateTablePresto(incExtSqlCreate, tableName, genDataDir,
						extTablePrefixRaw);
			saveCreateTableFile(workDir, subDir, "textfile", tableName, extSqlCreate);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if (tableName.equals("dbgen_version")) {
				System.out.println("Skipping: " + tableName);
				return;
			}
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + suffix);
			stmt.execute(extSqlCreate);
			if( doCount )
				countRowsQuery(stmt, tableName + suffix);
			String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "");
			String intSqlCreate = null;
			if( this.recorder.system.equals("hive") || this.recorder.system.equals("spark") )
				intSqlCreate = internalCreateTableHive(incIntSqlCreate, tableName, extTablePrefixCreated, format);
			else if( this.recorder.system.startsWith("presto") )
				intSqlCreate = internalCreateTablePresto(incIntSqlCreate, tableName, extTablePrefixCreated, format);
			saveCreateTableFile(workDir, subDir, format, tableName, intSqlCreate);
			stmt.execute("drop table if exists " + tableName);
			stmt.execute(intSqlCreate);
			if( this.recorder.system.equals("hive") || this.recorder.system.equals("spark") )
				stmt.execute("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix);
			else if( this.recorder.system.startsWith("presto") )
				stmt.execute("INSERT INTO " + tableName + " SELECT * FROM " + tableName + suffix);
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
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
	private String externalCreateTableHive(String incompleteSqlCreate, String tableName, String genDataDir,
			String extTablePrefixRaw) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' \n");
		builder.append("STORED AS TEXTFILE \n");
		if( extTablePrefixRaw == null )
			builder.append("LOCATION '" + genDataDir + "/" + tableName + "' \n");
		else
			builder.append("LOCATION '" + extTablePrefixRaw + "/" + genDataDir + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Presto.
	private String externalCreateTablePresto(String incompleteSqlCreate, String tableName, String genDataDir,
			String extTablePrefixRaw) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("WITH ( format = 'TEXTFILE', \n");
		if( extTablePrefixRaw == null )
			builder.append("external_location = '" + genDataDir + "/" + tableName + "' ) \n");
		else
			builder.append("external_location = '" + extTablePrefixRaw + "/" + tableName + "' ) \n");
		return builder.toString();
	}

	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	private String internalCreateTableHive(String incompleteSqlCreate, String tableName,
			String extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( extTablePrefixCreated == null ) {
			if( format.equals("parquet") )
				builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
			else if( format.equals("orc") )
				builder.append("STORED AS ORC TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\") \n");
		}
		else {
			if( format.equals("parquet") )
				builder.append("STORED AS PARQUET \n");
			else if( format.equals("orc") )
				builder.append("STORED AS ORC \n");
			builder.append("LOCATION '" + extTablePrefixCreated + "/" + tableName + "' \n");
			if( format.equals("parquet") )
				builder.append("TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
			else if( format.equals("orc") )
				builder.append("TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\") \n");
		}
		return builder.toString();
	}
	
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Presto.
	private String internalCreateTablePresto(String incompleteSqlCreate, String tableName,
			String extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( extTablePrefixCreated == null ) {
			if( format.equals("parquet") )
				builder.append("WITH ( format = 'PARQUET' ) \n");
			else if( format.equals("orc") )
				builder.append("WITH ( format = 'ORC' ) \n");
		}
		else {
			if( format.equals("parquet") )
				builder.append("WITH ( format = 'PARQUET', ");
			else if( format.equals("orc") )
				builder.append("WITH ( format = 'ORC', ");
			builder.append("external_location = '" + extTablePrefixCreated + "/" + tableName + "' ) \n");
		}
		return builder.toString();
	}

	
	public void saveCreateTableFile(String workDir, String subDir, String suffix, String tableName,
			String sqlCreate) {
		try {
			File temp = new File(workDir + "/" + subDir + suffix + "/" + tableName + ".sql");
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

	
	private void countRowsQuery(Statement stmt, String tableName) {
		try {
			String sql = "select count(*) from " + tableName;
			System.out.print("Running count query on " + tableName + ": ");
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		}
		catch (SQLException e) {
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
			this.con.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

}


