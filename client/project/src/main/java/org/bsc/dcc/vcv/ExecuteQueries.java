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
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String folderName;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String queriesDir;
	private final String resultsDir;
	private final String plansDir;
	private final boolean savePlans;
	private final boolean saveResults;
	private final String hostname;
	
	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * args[5] test name (e.g. power)
	 * args[6] experiment instance number
	 * args[7] queries dir
	 * args[8] subdirectory of work directory to store the results
	 * args[9] subdirectory of work directory to store the execution plans
	 * args[10] save plans (boolean)
	 * args[11] save results (boolean)
	 * args[12] hostname of the server
	 * args[13] "all" or query file
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public ExecuteQueries(String[] args) {
		this.workDir = args[0];
		this.dbName = args[1];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.queriesDir = args[7];
		this.resultsDir = args[8];
		this.plansDir = args[9];
		this.savePlans = Boolean.parseBoolean(args[10]);
		this.saveResults = Boolean.parseBoolean(args[11]);
		this.hostname = args[12];
		this.recorder = new AnalyticsRecorder(this.workDir, this.folderName, this.experimentName,
						this.system, this.test, this.instance);
		try {
			String driverName = "";
			if( this.system.equals("hive") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10000/" + this.dbName, "hive", "");
			}
			else if( this.system.equals("presto") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8080/hive/" + this.dbName, "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( this.system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hive", "");
				setPrestoDefaultSessionOpts();
			}
			else if( this.system.equals("sparkdatabricksjdbc") ) {
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://hostname:443/" + this.dbName);
			}
			else if( this.system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
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


	public static void main(String[] args) throws SQLException {
		if( args.length != 14 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Insufficient arguments: " + args.length);
			System.exit(1);
		}
		ExecuteQueries prog = new ExecuteQueries(args);
		String querySingleOrAllByNull = args[args.length-1].equalsIgnoreCase("all") ? null : args[args.length-1];
		prog.executeQueries(querySingleOrAllByNull);
		prog.closeConnection();
	}
	
	public void executeQueries(String querySingleOrAllByNull) {
		File directory = new File(this.workDir + "/" + this.queriesDir);
		// Process each .sql file found in the directory.
		// The preprocessing steps are necessary to obtain the right order, i.e.,
		// query1.sql, query2.sql, query3.sql, ..., query99.sql.
		File[] files = Stream.of(directory.listFiles()).
				map(File::getName).
				map(AppUtil::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				map(s -> new File(this.workDir + "/" + this.queriesDir + "/" + s)).
				toArray(File[]::new);
		this.recorder.header();
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				if( querySingleOrAllByNull != null ) {
					if( ! fileEntry.getName().equals(querySingleOrAllByNull) )
						continue;
				}
				this.executeQueryFile(fileEntry);
			}
		}
	}

	// Execute the query (or queries) from the provided file.
	private void executeQueryFile(File sqlFile) {
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
			this.executeQueryMultipleCalls(sqlFile.getName(), sqlStr, queryRecord);
			//Record the results file size.
			if( this.saveResults ) {
				String queryResultsFileName = this.generateResultsFileName(sqlFile.getName());
				File resultsFile = new File(queryResultsFileName);
				queryRecord.setResultsSize(resultsFile.length());
			}
			else
				queryRecord.setResultsSize(0);
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
	
	
	private String generateResultsFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.folderName + "/" + this.resultsDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	private String generatePlansFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.folderName + "/" + this.plansDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	// Execute the queries from the provided file.
	private void executeQueryMultipleCalls(String queryFileName, String sqlStrFull, QueryRecord queryRecord) 
			throws Exception {
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
				this.saveResults(this.generatePlansFileName(queryFileName), planrs, ! firstQuery);
			planrs.close();
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + queryFileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			//this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs, ! firstQuery);
			if( this.saveResults )
				this.saveResults(this.generateResultsFileName(queryFileName), rs, ! firstQuery);
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
