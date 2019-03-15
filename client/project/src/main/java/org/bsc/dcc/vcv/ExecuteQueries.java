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
import java.util.StringTokenizer;
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
	public ExecuteQueries(String system, String hostname) {
		try {
			system = system.toLowerCase();
			String driverName = "";
			if( system.equals("hive") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10000/default", "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/default", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/default", "", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
			this.recorder = new AnalyticsRecorder("power", system);
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
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
	 * args[5] hostname of the server
	 * args[6] OPTIONAL: query file
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws SQLException {
		ExecuteQueries prog = new ExecuteQueries(args[4], args[5]);
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
		String queryFile = args.length >= 7 ? args[6] : null;
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				if( queryFile != null ) {
					if( ! fileEntry.getName().equals(queryFile) )
						continue;
				}
				prog.executeQueryFile(args[0], fileEntry, args[2], args[3], false);
			}
		}
		prog.closeConnection();
	}

	// Execute the query (or queries) from the provided file.
	private void executeQueryFile(String workDir, File sqlFile, String resultsDir,
			String plansDir, boolean singleCall) {
		QueryRecord queryRecord = null;
		try {
			this.logger.info("Processing: " + sqlFile.getName());
			String fileName = sqlFile.getName().substring(0, sqlFile.getName().indexOf('.'));
			String nQueryStr = fileName.replaceAll("[^\\d.]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			queryRecord = new QueryRecord(nQuery);
			String sqlStr = readFileContents(sqlFile.getAbsolutePath());
			//Execute the query or queries.
			if( singleCall )
				this.executeQuerySingleCall(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
			else
				this.executeQueryMultipleCalls(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
			//Record the results file size.
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + "power" + "/" + 
					this.recorder.system + "/" + fileName + ".txt");
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
	
	// Execute a query from the provided file.
	private void executeQuerySingleCall(String workDir, String resultsDir, String plansDir, 
			String fileName, String sqlStr, QueryRecord queryRecord)
			throws SQLException {
		// Remove the last semicolon.
		sqlStr = sqlStr.trim();
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);
		// Obtain the plan for the query.
		Statement stmt = con.createStatement();
		ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
		this.saveResults(workDir + "/" + plansDir + "/" + fileName + ".txt", planrs, false);
		planrs.close();
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		ResultSet rs = stmt.executeQuery(sqlStr);
		// Save the results.
		this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs, false);
		stmt.close();
		rs.close();
	}
	
	// Execute the queries from the provided file.
	private void executeQueryMultipleCalls(String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStrFull, QueryRecord queryRecord) throws SQLException {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;
			// Obtain the plan for the query.
			Statement stmt = con.createStatement();
			ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
			//this.saveResults(workDir + "/" + plansDir + "/" + fileName + ".txt", planrs, ! firstQuery);
			this.saveResults(workDir + "/" + plansDir + "/" + "power" + "/" + this.recorder.system + "/" +
					fileName + ".txt", planrs, ! firstQuery);
			planrs.close();
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + fileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			//this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs, ! firstQuery);
			this.saveResults(workDir + "/" + resultsDir + "/" + "power" + "/" + this.recorder.system + "/" + 
					fileName + ".txt", rs, ! firstQuery);
			stmt.close();
			rs.close();
			firstQuery = false;
			iteration++;
		}
	}

	private void saveResults(String resFileName, ResultSet rs, boolean append) {
		try {
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName, append);
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
