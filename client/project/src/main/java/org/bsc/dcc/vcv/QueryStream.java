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
	private Random random;
	private ExecuteQueriesConcurrent parent;

	
	public QueryStream(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			Connection con, HashMap<Integer, String> queriesHT, int nQueries, Random random,
			ExecuteQueriesConcurrent parent) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.con = con;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.random = random;
		this.parent = parent;
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
		//int[] impalaKit = {19, 27, 3, 34, 42, 43, 46, 52, 53, 55, 59, 63, 65, 68, 7, 73, 79, 8,  82,  89, 98};
		//Arrays.sort(impalaKit);
		for(int i = 0; i < queries.length; i++) {
			//if( queries[i] == 6 || queries[i] == 9 || queries[i] == 10 || queries[i] == 35 || 
			//		queries[i] == 41 || queries[i] == 66 || queries[i] == 69 || queries[i] == 87 )
			//continue;
			//if( Arrays.binarySearch(impalaKit, queries[i]) < 0 )
			//	continue;
			String sqlStr = this.queriesHT.get(queries[i]);
			this.executeQuery(this.nStream, queries[i], sqlStr, i);
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
			queryRecord = new QueryRecordConcurrent(nStream, nQuery);
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
			ResultSet planrs = stmt.executeQuery("EXPLAIN " + sqlStr);
			if( this.parent.savePlans )
				this.saveResults(generatePlansFileName(fileName, nStream, item), planrs, !firstQuery);
			planrs.close();
			// Execute the query.
			if (firstQuery)
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Stream " + nStream + " item " + item + 
					" executing iteration " + iteration + " of query " + fileName + ".");
			ResultSet rs = stmt.executeQuery(sqlStr);
			// Save the results.
			if( this.parent.saveResults )
				this.saveResults(generateResultsFileName(fileName, nStream, item), rs, !firstQuery);
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

