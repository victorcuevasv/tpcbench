package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class QueryStream implements Callable<Void> {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private Connection con;
	private int nStream;
	private HashMap<Integer, String> queriesHT;
	private int nQueries;
	private String workDir;
	private String resultsDir;
	private String plansDir;
	private boolean singleCall;
	private Random random;
	private final String system;
	boolean savePlans;
	boolean saveResults;

	public QueryStream(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			Connection con, HashMap<Integer, String> queriesHT, int nQueries,
			String workDir, String resultsDir, String plansDir, boolean singleCall, 
			Random random, String system, boolean savePlans, boolean saveResults) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.con = con;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.workDir = workDir;
		this.resultsDir = resultsDir;
		this.plansDir = plansDir;
		this.singleCall = singleCall;
		this.random = random;
		this.system = system;
		this.savePlans = savePlans;
		this.saveResults = saveResults;
	}
	
	private void setPrestoDefaultSessionOpts() {
		((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
		((PrestoConnection)con).setSessionProperty("join_reordering_strategy", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("join_distribution_type", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("task_concurrency", "8");
	}

	@Override
	public Void call() {
		//Integer[] queries = this.queriesHT.keySet().toArray(new Integer[] {});
		//Arrays.sort(queries);
		//this.shuffle(queries);
		int[] queries = StreamsTable.matrix[this.nStream];
		for(int i = 0; i < queries.length; i++) {
			String sqlStr = this.queriesHT.get(queries[i]);
			this.executeQuery(this.nStream, this.workDir, queries[i], sqlStr,
					this.resultsDir, this.plansDir, this.singleCall, i);
		}
		return null;
	}

	// Execute the query (or queries) from the provided file.
	private void executeQuery(int nStream, String workDir, int nQuery, String sqlStr, String resultsDir,
			String plansDir, boolean singleCall, int item) {
		QueryRecordConcurrent queryRecord = null;
		String fileName = "query" + nQuery;
		try {
			if( this.system.equals("prestoemr") ) {
				this.setPrestoDefaultSessionOpts();
			}
			queryRecord = new QueryRecordConcurrent(nStream, nQuery);
			// Execute the query or queries.
			if (singleCall)
				this.executeQuerySingleCall(nStream, workDir, resultsDir, plansDir,
						fileName, sqlStr, queryRecord);
			else
				this.executeQueryMultipleCalls(nStream, workDir, resultsDir, plansDir,
						fileName, sqlStr, queryRecord, item);
			// Record the results file size.
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + "tput" + "/" + 
					this.system + "/" + nStream + "_" + fileName + ".txt");
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
			this.resultsQueue.add(queryRecord);
		}
	}

	// Execute a query from the provided file.
	private void executeQuerySingleCall(int nStream, String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStr, QueryRecordConcurrent queryRecord) throws SQLException {
		// Remove the last semicolon.
		sqlStr = sqlStr.trim();
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);
		// Obtain the plan for the query.
		Statement stmt = con.createStatement();
		ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
		this.saveResults(workDir + "/" + plansDir + "/" + nStream + "_" + fileName + ".txt", planrs, false);
		planrs.close();
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		ResultSet rs = stmt.executeQuery(sqlStr);
		// Save the results.
		this.saveResults(workDir + "/" + resultsDir + "/" + nStream + "_" + fileName + ".txt", rs, false);
		stmt.close();
		rs.close();
	}

	// Execute the queries from the provided file.
	private void executeQueryMultipleCalls(int nStream, String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStrFull, QueryRecord queryRecord, int item) throws SQLException {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if (sqlStr.length() == 0)
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
			if( this.savePlans )
				this.saveResults(workDir + "/" + plansDir + "/" + "tput" + "/" + this.system + "/" + 
					nStream + "_" + item + "_" + fileName + ".txt", planrs, !firstQuery);
			planrs.close();
			// Execute the query.
			if (firstQuery)
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Stream " + nStream + " item " + item + 
					" executing iteration " + iteration + " of query " + fileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			if( this.saveResults )
				this.saveResults(workDir + "/" + resultsDir + "/" + "tput" + "/" + this.system + "/" + 
					nStream + "_" + item + "_" + fileName + ".txt", rs, !firstQuery);
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
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
			this.logger.error(AppUtil.stringifyStackTrace(ioe));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private void shuffle(Integer[] array) {
		for(int i = 0; i < array.length; i++) {
			int randPos = this.random.nextInt(array.length);
			int temp = array[i];
			array[i] = array[randPos];
			array[randPos] = temp;
		}
	}

}

