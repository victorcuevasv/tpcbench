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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;


public class QueryWorkerLimit implements Callable<Void> {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final Connection con;
	private final HashMap<Integer, String> queriesHT;
	private final Random random;
	private final ExecuteQueriesConcurrentLimit parent;
	private final int nWorker;
	private final int totalQueries;
	private final List<Semaphore> semaphores;

	
	public QueryWorkerLimit(int nWorker, BlockingQueue<QueryRecordConcurrent> queriesQueue,
			BlockingQueue<QueryRecordConcurrent> resultsQueue, Connection con,
			HashMap<Integer, String> queriesHT, int totalQueries, Random random,
			ExecuteQueriesConcurrentLimit parent, List<Semaphore> semaphores) {
		this.nWorker = nWorker;
		this.queriesQueue = queriesQueue;
		this.resultsQueue = resultsQueue;
		this.con = con;
		this.queriesHT = queriesHT;
		this.totalQueries = totalQueries;
		this.random = random;
		this.parent = parent;
		this.semaphores = semaphores;
	}
	
	
	private void setPrestoDefaultSessionOpts() {
		((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
		((PrestoConnection)con).setSessionProperty("join_reordering_strategy", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("join_distribution_type", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("task_concurrency", "8");
	}
	
	
	@Override
	public Void call() {
		while( this.parent.atomicCounter.get() < this.totalQueries  ) {
			try {
				QueryRecordConcurrent queryRecord = this.queriesQueue.poll(10L, TimeUnit.SECONDS);
				if( queryRecord == null )
					break;
				//It is assumed that the query is available in the hash table.
				String sqlStr = this.queriesHT.get(queryRecord.getQuery());
				this.executeQuery(queryRecord.getStream(), queryRecord.getQuery(), sqlStr, 
						queryRecord.getItem());
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	
	private String generateResultsFileName(String fileName, int nStream, int item) {
		return this.parent.workDir + "/" + this.parent.folderName + "/" + this.parent.resultsDir + 
				"/" + this.parent.experimentName + "/" + this.parent.test + "/" + this.parent.instance + "/" + 
				nStream + "_" + item + "_" + fileName + ".txt";
	}
	
	
	private String generatePlansFileName(String fileName, int nStream, int item) {
		return this.parent.workDir + "/" + this.parent.folderName + "/" + this.parent.plansDir + 
				"/" + this.parent.experimentName + "/" + this.parent.test + "/" + this.parent.instance + "/" + 
				nStream + "_" + item + "_" + fileName + ".txt";
	}
	
	
	// Execute the query (or queries) from the provided file.
	private void executeQuery(int nStream, int nQuery, String sqlStr, int item) {
		QueryRecordConcurrent queryRecord = null;
		String fileName = "query" + nQuery;
		try {
			if( this.parent.system.equals("prestoemr") ) {
				this.setPrestoDefaultSessionOpts();
			}
			queryRecord = new QueryRecordConcurrent(nStream, nQuery, item);
			// Execute the query or queries.
			this.executeQueryMultipleCalls(nStream, fileName, sqlStr, queryRecord, item);
			// Record the results file size.
			if( this.parent.saveResults ) {
				File resultsFile = new File(generateResultsFileName(fileName, nStream, item));
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
			this.resultsQueue.add(queryRecord);
			this.semaphores.get(queryRecord.getStream()).release();
		}
	}
	

	// Execute the queries from the provided file.
	private void executeQueryMultipleCalls(int nStream, String fileName, String sqlStrFull,
			QueryRecord queryRecord, int item) throws SQLException {
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
			if( this.parent.savePlans ) {
				ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
				this.saveResults(generatePlansFileName(fileName, nStream, item), planrs, !firstQuery);
				planrs.close();
			}
			// Execute the query.
			if (firstQuery)
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Stream " + nStream + " item " + item + 
					" executing iteration " + iteration + " of query " + fileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			if( this.parent.saveResults ) {
				int tuples = this.saveResults(generateResultsFileName(fileName, nStream, item), rs, !firstQuery);
				queryRecord.setTuples(queryRecord.getTuples() + tuples);
			}
			stmt.close();
			rs.close();
			firstQuery = false;
			iteration++;
		}
	}

	
	private int saveResults(String resFileName, ResultSet rs, boolean append) {
		int tuples = 0;
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
				tuples++;
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
		return tuples;
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

