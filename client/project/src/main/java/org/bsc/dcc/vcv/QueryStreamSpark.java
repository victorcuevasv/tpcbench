package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Random;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class QueryStreamSpark implements Callable<Void> {

	private static final Logger logger = LogManager.getLogger(QueryStreamSpark.class);
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private SparkSession spark;
	private int nStream;
	private HashMap<Integer, String> queriesHT;
	private int nQueries;
	private String workDir;
	private String resultsDir;
	private String plansDir;
	private boolean singleCall;
	private Random random;

	public QueryStreamSpark(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			SparkSession spark, HashMap<Integer, String> queriesHT, int nQueries,
			String workDir, String resultsDir, String plansDir, boolean singleCall, Random random) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.spark = spark;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.workDir = workDir;
		this.resultsDir = resultsDir;
		this.plansDir = plansDir;
		this.singleCall = singleCall;
		this.random = random;
	}

	@Override
	public Void call() {
		Integer[] queries = this.queriesHT.keySet().toArray(new Integer[] {});
		this.shuffle(queries);
		for(int i = 0; i < nQueries; i++) {
			String sqlStr = this.queriesHT.get(queries[i]);
			this.executeQuery(this.nStream, this.workDir, queries[i], sqlStr,
					this.resultsDir, this.plansDir, this.singleCall);
		}
		return null;
	}

	// Execute the query (or queries) from the provided file.
	private void executeQuery(int nStream, String workDir, int nQuery, String sqlStr, String resultsDir,
			String plansDir, boolean singleCall) {
		QueryRecordConcurrent queryRecord = null;
		String noExtFileName = "query" + nQuery;
		try {
			queryRecord = new QueryRecordConcurrent(nStream, nQuery);
			// Execute the query or queries.
			if (singleCall)
				this.executeQuerySingleCall(nStream, workDir, resultsDir, plansDir,
						noExtFileName, sqlStr, queryRecord);
			else
				this.executeQueryMultipleCalls(nStream, workDir, resultsDir, plansDir,
						noExtFileName, sqlStr, queryRecord);
			// Record the results file size.
			long resultsSize = calculateSize(
					workDir + "/" + resultsDir + "/" + nStream + "_" + noExtFileName, ".csv");
			queryRecord.setResultsSize(resultsSize);
			queryRecord.setSuccessful(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.resultsQueue.add(queryRecord);
		}
	}
	
	// Execute a query from the provided file.
	private void executeQuerySingleCall(int nStream, String workDir, String resultsDir, String plansDir, 
				String noExtFileName, String sqlStr, QueryRecordConcurrent queryRecord) {
		// Remove the last semicolon.
		sqlStr = sqlStr.trim();
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);
		// Obtain the plan for the query.
		Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
		planDataset.write().mode(SaveMode.Overwrite).csv(
				workDir + "/" + plansDir + "/" + nStream + "_" + noExtFileName);
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		Dataset<Row> dataset = this.spark.sql(sqlStr);
		// Save the results.
		dataset.write().mode(SaveMode.Overwrite).csv(
				workDir + "/" + resultsDir + "/" + nStream + "_" + noExtFileName);
	}
	
	private void executeQueryMultipleCalls(int nStream, String workDir, String resultsDir, String plansDir,
			String noExtFileName, String sqlStrFull, QueryRecordConcurrent queryRecord) {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;
			// Obtain the plan for the query.
			Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			planDataset.write().mode(SaveMode.Overwrite).csv(
					workDir + "/" + plansDir + "/" + nStream + "_" + noExtFileName);
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + noExtFileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			if( firstQuery )
				dataset.write().mode(SaveMode.Overwrite).csv(
						workDir + "/" + resultsDir + "/" + nStream + "_" + noExtFileName);
			else
				dataset.write().mode(SaveMode.Append).csv(
						workDir + "/" + resultsDir + "/" + nStream + "_" + noExtFileName);
			firstQuery = false;
			iteration++;
		}
	}
	
	/*
	 * Calculate the size of the files ending with a given extension and stored in a given folder.
	 * 
	 * Since the operation is non-atomic, the returned value may be inaccurate.
	 * However, this method is quick and does its best.
	 */
	public static long calculateSize(String pathStr, String extension) {

	    final AtomicLong size = new AtomicLong(0);
	    Path path = Paths.get(pathStr);
	    try {
	        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
	            @Override
	            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
	            	if( file.toString().endsWith(extension) )
	            		size.addAndGet(attrs.size());
	                return FileVisitResult.CONTINUE;
	            }
	            @Override
	            public FileVisitResult visitFileFailed(Path file, IOException exc) {

	                System.out.println("skipped: " + file + " (" + exc + ")");
	                // Skip folders that can't be traversed.
	                return FileVisitResult.CONTINUE;
	            }
	            @Override
	            public FileVisitResult postVisitDirectory(Path dir, IOException ioe) {

	                if (ioe != null)
	                    System.out.println("Error when traversing: " + dir + " (" + ioe + ")");
	                // Ignore errors traversing a folder.
	                return FileVisitResult.CONTINUE;
	            }
	        });
	    } 
	    catch (IOException e) {
	        e.printStackTrace();
	        this.logger.error(e);
	    }
	    return size.get();
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

