package org.bsc.dcc.vcv;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;


public class ExecuteQueriesSpark {
	
	//private static final Logger logger = LogManager.getLogger(ExecuteQueriesSpark.class);
	private SparkSession spark;
	private AnalyticsRecorder recorder;
	private JarQueriesReaderAsResource queriesReader;

	public ExecuteQueriesSpark() {
		this.queriesReader = new JarQueriesReaderAsResource();
		this.spark = SparkSession.builder().appName("Java Spark Hive Example")
				.enableHiveSupport()
				.getOrCreate();
		this.recorder = new AnalyticsRecorder();
	}

	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory to store the results
	 * args[2] subdirectory of work directory to store the execution plans
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) {
		ExecuteQueriesSpark prog = new ExecuteQueriesSpark();
		prog.executeQueries(args[0], args[1], args[2]);
	}
	
	public void executeQueries(String workDir, String resultsDir, String plansDir) {
		this.recorder.header();
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			String sqlStr = this.queriesReader.getFile(fileName);
			String nQueryStr = fileName.replaceAll("[^\\d.]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			QueryRecord queryRecord = new QueryRecord(nQuery);
			if( ! fileName.equals("query1.sql") )
				continue;
			System.out.println("\n\n\n\n\n---------------------------------------------------------");
			System.out.println(sqlStr);
			System.out.println("\n\n\n\n\n---------------------------------------------------------");
			this.executeQuerySingleCall(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
		}
		this.spark.stop();
	}
	
	// Execute a query from the provided file.
	private void executeQuerySingleCall(String workDir, String resultsDir, String plansDir, 
			String fileName, String sqlStr, QueryRecord queryRecord) {
		// Remove the last semicolon.
		sqlStr = sqlStr.trim();
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);
		// Obtain the plan for the query.
		//ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
		//this.saveResults(workDir + "/" + plansDir + "/" + fileName + ".txt", planrs, false);
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		this.spark.sql(sqlStr).show();
		// Save the results.
		//this.saveResults(workDir + "/" + resultsDir + "/" + fileName + ".txt", rs, false);
	}

}

