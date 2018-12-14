package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.sql.DriverManager;
import java.io.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class ExecuteQueries {

	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger(ExecuteQueries.class);
	private AnalyticsRecorder recorder;

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public ExecuteQueries(String system) {
		try {
			system = system.toLowerCase();
			String driverName = "";
			if( system.equals("hive") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://hiveservercontainer:10000/default", "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://hiveservercontainer:8080/hive/default", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
			this.recorder = new AnalyticsRecorder();
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory that contains the queries
	 * args[2] subdirectory of work directory to store the results
	 * args[3] subdirectory of work directory to store the execution plans
	 * args[4] system to evaluate the queries (hive/presto)
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws SQLException {
		ExecuteQueries prog = new ExecuteQueries(args[4]);
		File directory = new File(args[0] + "/" + args[1]);
		// Process each .sql file found in the directory.
		// The preprocessing steps are necessary to obtain the right order, i.e.,
		// query1.sql, query2.sql, query3.sql, ..., query99.sql.
		File[] files = Stream.of(directory.listFiles()).
				map(File::getName).
				map(ExecuteQueries::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				map(s -> new File(args[0] + "/" + args[1] + "/" + s)).
				toArray(File[]::new);
		prog.recorder.header();
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				if( ! fileEntry.getName().equals("query14.sql") )
					continue;
				prog.executeQuery(args[0], fileEntry, args[2], args[3]);
			}
		}
		prog.closeConnection();
	}

	// Execute a query from the provided file.
	private void executeQuery(String workDir, File sqlFile, String resultsDir,
			String plansDir) {
		QueryRecord queryRecord = null;
		try {
			this.logger.info("Processing: " + sqlFile.getName());
			String fileName = sqlFile.getName().substring(0, sqlFile.getName().indexOf('.'));
			String nQueryStr = fileName.replaceAll("[^\\d.]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			queryRecord = new QueryRecord(nQuery);
			String sqlQuery = readFileContents(sqlFile.getAbsolutePath());
			// Remove the last semicolon.
			sqlQuery = sqlQuery.trim();
			sqlQuery = sqlQuery.substring(0, sqlQuery.length() - 1);
			// Obtain the plan for the query.
			Statement stmt = con.createStatement();
			ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlQuery);
			this.saveResults(workDir + "/" + plansDir + "/" + fileName + ".txt", planrs);
			planrs.close();
			// Execute the query.
			queryRecord.setStartTime(System.currentTimeMillis());
			ResultSet rs = stmt.executeQuery(sqlQuery);
			// Save the results.
			this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs);
			stmt.close();
			rs.close();
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + fileName + ".txt");
			queryRecord.setResultsSize(resultsFile.length());
			queryRecord.setSuccessful(true);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.recorder.record(queryRecord);
		}
	}

	private void saveResults(String resFileName, ResultSet rs) {
		try {
			FileWriter fileWriter = new FileWriter(resFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			ResultSetMetaData metadata = rs.getMetaData();
			int nCols = metadata.getColumnCount();
			while (rs.next()) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int i = 1; i <= nCols; i++) {
					rowBuilder.append(rs.getString(i) + ", ");
				}
				printWriter.println(rowBuilder.toString());
			}
			printWriter.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
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
	
	private void closeConnection() {
		try {
			this.con.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	// Converts a string representing a filename like query12.sql to the integer 12.
	public static int extractNumber(String fileName) {
		String nStr = fileName.substring(0, fileName.indexOf('.')).replaceAll("[^\\d.]", "");
		return Integer.parseInt(nStr);
	}

}
