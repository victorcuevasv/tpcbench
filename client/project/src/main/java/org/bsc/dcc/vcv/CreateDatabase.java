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
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorder recorder;

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CreateDatabase(String hostname, String system) {
		try {
			if( system.equals("hive") ) {
				Class.forName(driverName);
				// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
				// "hive", "");
				con = DriverManager.getConnection("jdbc:hive2://" + hostname + 
					":10000/default", "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/default", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8889/hive/default", "hadoop", "");
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + system);
			}
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		this.recorder = new AnalyticsRecorder("load", system);
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] subdirectory within main work directory with the create table files
	 * args[1] suffix used for intermediate table text files
	 * args[2] directory for generated data raw files
	 * args[3] hostname of the server
	 * args[4] system running the data loading queries
	 * args[5] count (boolean) whether to run queries to count the tuples generated
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length != 6 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(0);
		}
		CreateDatabase prog = new CreateDatabase(args[3], args[4]);
		boolean doCount = Boolean.parseBoolean(args[5]);
		File directory = new File(args[0]);
		prog.recorder.header();
		// Process each .sql create table file found in the directory.
		File[] filesArray = directory.listFiles();
		List<File> filesList = Arrays.stream(filesArray).sorted().collect(Collectors.toList());
		int i = 1;
		for (final File fileEntry : filesList) {
			if (!fileEntry.isDirectory()) {
				prog.createTable(args[0], fileEntry, args[1], args[2], doCount, i);
				i++;
			}
		}
		prog.closeConnection();
	}

	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be manipulated for
	// creating these tables.
	private void createTable(String workDir, File tableSQLfile, String suffix, String genDataDir,
			boolean doCount, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = tableSQLfile.getName().substring(0, tableSQLfile.getName().indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String sqlCreate = readFileContents(tableSQLfile.getAbsolutePath());
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, 
					! this.recorder.system.startsWith("presto"), suffix);
			String extSqlCreate = null;
			if( this.recorder.system.equals("hive") )
				extSqlCreate = externalCreateTableHive(incExtSqlCreate, tableName, genDataDir);
			else if( this.recorder.system.startsWith("presto") )
				extSqlCreate = externalCreateTablePresto(incExtSqlCreate, tableName, genDataDir);
			saveCreateTableFile(workDir, "textfile", tableName, extSqlCreate);
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
			if( this.recorder.system.equals("hive") )
				intSqlCreate = internalCreateTableHive(incIntSqlCreate, tableName);
			else if( this.recorder.system.startsWith("presto") )
				intSqlCreate = internalCreateTablePresto(incIntSqlCreate, tableName);
			saveCreateTableFile(workDir, "parquet", tableName, intSqlCreate);
			stmt.execute("drop table if exists " + tableName);
			stmt.execute(intSqlCreate);
			if( this.recorder.system.equals("hive") )
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
	private String externalCreateTableHive(String incompleteSqlCreate, String tableName, String genDataDir) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' \n");
		builder.append("STORED AS TEXTFILE \n");
		builder.append("LOCATION '" + genDataDir + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Presto.
	private String externalCreateTablePresto(String incompleteSqlCreate, String tableName, String genDataDir) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("WITH ( format = 'TEXTFILE', \n");
		builder.append("external_location = '" + genDataDir + "/" + tableName + "' ) \n");
		return builder.toString();
	}

	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	private String internalCreateTableHive(String incompleteSqlCreate, String tableName) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
		return builder.toString();
	}
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Presto.
	private String internalCreateTablePresto(String incompleteSqlCreate, String tableName) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("WITH ( format = 'PARQUET' ) \n");
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
			this.logger.error(e);
		}
	}

}


