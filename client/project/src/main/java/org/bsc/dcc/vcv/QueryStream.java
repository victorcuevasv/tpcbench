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
import java.util.HashMap;

public class QueryStream implements Callable<Void> {

	private BlockingQueue<QueryRecord> resultsQueue;
	private Connection con;
	private int nStream;
	private HashMap<Integer, String> queriesHT;
	private int nQueries;
	private String workDir;
	private String resultsDir;
	private String plansDir;
	private boolean singleCall;

	public QueryStream(int nStream, BlockingQueue<QueryRecord> resultsQueue,
			Connection con, HashMap<Integer, String> queriesHT, int nQueries,
			String workDir, String resultsDir, String plansDir, boolean singleCall) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.con = con;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.workDir = workDir;
		this.resultsDir = resultsDir;
		this.plansDir = plansDir;
		this.singleCall = singleCall;
	}

	@Override
	public Void call() {
		for(int i = 1; i <= nQueries; i++) {
			String sqlStr = this.queriesHT.get(i);
			this.executeQuery(this.nStream, this.workDir, i, sqlStr,
					this.resultsDir, this.plansDir, this.singleCall);
		}
		return null;
	}

	// Execute the query (or queries) from the provided file.
	private void executeQuery(int nStream, String workDir, int nQuery, String sqlStr, String resultsDir,
			String plansDir, boolean singleCall) {
		QueryRecord queryRecord = null;
		String fileName = "query" + nQuery;
		try {
			queryRecord = new QueryRecord(nQuery);
			// Execute the query or queries.
			if (singleCall)
				this.executeQuerySingleCall(nStream, workDir, resultsDir, plansDir,
						fileName, sqlStr, queryRecord);
			else
				this.executeQueryMultipleCalls(nStream, workDir, resultsDir, plansDir,
						fileName, sqlStr, queryRecord);
			// Record the results file size.
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + nStream + "_" + fileName + ".txt");
			queryRecord.setResultsSize(resultsFile.length());
			queryRecord.setSuccessful(true);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.resultsQueue.add(queryRecord);
		}
	}

	// Execute a query from the provided file.
	private void executeQuerySingleCall(int nStream, String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStr, QueryRecord queryRecord) throws SQLException {
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
			String fileName, String sqlStrFull, QueryRecord queryRecord) throws SQLException {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if (sqlStr.length() == 0)
				continue;
			// Obtain the plan for the query.
			Statement stmt = con.createStatement();
			ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
			this.saveResults(workDir + "/" + plansDir + "/" + nStream + "_" + fileName + ".txt", planrs, !firstQuery);
			planrs.close();
			// Execute the query.
			if (firstQuery)
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + fileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			this.saveResults(workDir + "/" + resultsDir + "/" + nStream + "_" + fileName + ".txt", rs, !firstQuery);
			stmt.close();
			rs.close();
			firstQuery = false;
			iteration++;
		}
	}

	private void saveResults(String resFileName, ResultSet rs, boolean append) {
		try {
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
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

}

