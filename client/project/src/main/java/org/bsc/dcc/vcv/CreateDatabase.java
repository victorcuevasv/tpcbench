package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateDatabase {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger(CreateDatabase.class);

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CreateDatabase() {
		try {
			Class.forName(driverName);
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
			con = DriverManager.getConnection("jdbc:hive2://hiveservercontainer:10000/default", "hive", "");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		CreateDatabase prog = new CreateDatabase();
		File directory = new File(args[0]);
		// Process each .dat file found in the directory.
		for (final File fileEntry : directory.listFiles()) {
			if (!fileEntry.isDirectory()) {
				prog.createTable(args[0], fileEntry, args[1]);
			}
		}
	}

	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be manipulated for
	// creating these tables.
	private void createTable(String workDir, File tableSQLfile, String suffix) {
		try {
			String tableName = tableSQLfile.getName().substring(0, tableSQLfile.getName().indexOf('.'));
			System.out.println("Processing: " + tableName);
			String sqlCreate = readFileContents(tableSQLfile.getAbsolutePath());
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, true, suffix);
			String extSqlCreate = externalCreateTable(incExtSqlCreate, tableName);
			saveCreateTableFile(workDir, "textfile", tableName, extSqlCreate);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if (tableName.equals("dbgen_version")) {
				System.out.println("Skipping: " + tableName);
				return;
			}
			Statement stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + suffix);
			stmt.execute(extSqlCreate);
			countRowsQuery(stmt, tableName + suffix);
			String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "");
			String intSqlCreate = internalCreateTable(incIntSqlCreate, tableName);
			saveCreateTableFile(workDir, "parquet", tableName, intSqlCreate);
			stmt.execute("drop table if exists " + tableName);
			stmt.execute(intSqlCreate);
			stmt.execute("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix);
			countRowsQuery(stmt, tableName);

		} catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
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
		return builder.toString();
	}

	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Hive.
	private String externalCreateTable(String incompleteSqlCreate, String tableName) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' \n");
		builder.append("STORED AS TEXTFILE \n");
		builder.append("LOCATION '/tmp/1GB/" + tableName + "' \n");
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

}
