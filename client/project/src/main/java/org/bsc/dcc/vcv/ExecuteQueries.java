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
	private static final String databricksDriverName = "com.simba.spark.jdbc41.Driver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorder recorder;
	boolean savePlans;
	boolean saveResults;

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public ExecuteQueries(String system, String hostname, boolean savePlans, boolean saveResults,
			String dbName) {
		try {
			this.savePlans = savePlans;
			this.saveResults = saveResults;
			system = system.toLowerCase();
			String driverName = "";
			if( system.equals("hive") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10000/" + dbName, "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/" + dbName, "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8889/hive/" + dbName, "hive", "");
				setPrestoDefaultSessionOpts();
			}
			else if( system.equals("sparkdatabricksjdbc") ) {
				Class.forName(databricksDriverName);
				con = DriverManager.getConnection("jdbc:hive2://hostname:443/" + dbName);
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/" + dbName, "", "");
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
	
	private void setPrestoDefaultSessionOpts() {
		((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
		((PrestoConnection)con).setSessionProperty("join_reordering_strategy", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("join_distribution_type", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("task_concurrency", "8");
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
	 * args[6] save plans (boolean)
	 * args[7] save results (boolean)
	 * args[8] database name
	 * args[9] "all" or query file
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length < 10 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		boolean savePlans = Boolean.parseBoolean(args[6]);
		boolean saveResults = Boolean.parseBoolean(args[7]);
		ExecuteQueries prog = new ExecuteQueries(args[4], args[5], savePlans, saveResults, args[8]);
		File directory = new File(args[0] + "/" + args[1]);
		// Process each .sql file found in the directory.
		// The preprocessing steps are necessary to obtain the right order, i.e.,
		// query1.sql, query2.sql, query3.sql, ..., query99.sql.
		File[] files = Stream.of(directory.listFiles()).
				map(File::getName).
				map(AppUtil::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				map(s -> new File(args[0] + "/" + args[1] + "/" + s)).
				toArray(File[]::new);
		prog.recorder.header();
		String queryFile = args[args.length-1].equalsIgnoreCase("all") ? null : args[args.length-1];
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
			String fileName = sqlFile.getName().substring(0, sqlFile.getName().indexOf('.'));
			String nQueryStr = fileName.replaceAll("[^\\d.]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			queryRecord = new QueryRecord(nQuery);
			String sqlStr = readFileContents(sqlFile.getAbsolutePath());
			this.logger.info("\nExecuting query: " + sqlFile.getName() + "\n");
			if( this.recorder.system.equals("prestoemr") ) {
				this.setPrestoDefaultSessionOpts();
			}
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
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.recorder.record(queryRecord);
		}
	}
	
	// Execute a query from the provided file.
	private void executeQuerySingleCall(String workDir, String resultsDir, String plansDir, 
			String fileName, String sqlStr, QueryRecord queryRecord)
			throws Exception {
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
			String fileName, String sqlStrFull, QueryRecord queryRecord) throws Exception {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;	
			if( sqlStr.contains("SET SESSION") ) {
				Statement sessionStmt = con.createStatement();
				sessionStmt.executeUpdate(sqlStr);
				sessionStmt.close();
				continue;
			}
			// Obtain the plan for the query.
			Statement stmt = con.createStatement();
			ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
			//this.saveResults(workDir + "/" + plansDir + "/" + fileName + ".txt", planrs, ! firstQuery);
			if( this.savePlans )
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
			if( this.saveResults )
				this.saveResults(workDir + "/" + resultsDir + "/" + "power" + "/" + this.recorder.system + "/" + 
					fileName + ".txt", rs, ! firstQuery);
			stmt.close();
			rs.close();
			firstQuery = false;
			iteration++;
		}
	}

	private void saveResults(String resFileName, ResultSet rs, boolean append) 
			throws Exception {
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

}
