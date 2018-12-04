package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecuteQueries {

	private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger(ExecuteQueries.class);
	private AnalyticsRecorder recorder;

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public ExecuteQueries() {
		try {
			Class.forName(driverName);
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
			con = DriverManager.getConnection("jdbc:hive2://hiveservercontainer:10000/default", "hive", "");
			this.recorder = new AnalyticsRecorder();
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
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory that contains the queries
	 * args[2] subdirectory of work directory to store the results
	 * 
	 * all of them without slash
	 */
	public static void main(String[] args) throws SQLException {
		ExecuteQueries prog = new ExecuteQueries();
		File directory = new File(args[0] + "/" + args[1]);
		// Process each .sql file found in the directory.
		File[] files = directory.listFiles();
		Arrays.sort(files);
		prog.recorder.header();
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				prog.executeQuery(args[0], fileEntry, args[2]);
			}
		}
	}

	// Execute a query from the provided file.
	private void executeQuery(String workDir, File sqlFile, String resultsDir) {
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
			// Execute the query.
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sqlQuery);
			// Save the results.
			this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs);
			queryRecord.setSuccessful(true);
		}
		catch (SQLException e) {
			e.printStackTrace();
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
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
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
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return retVal;
	}

}
