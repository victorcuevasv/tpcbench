package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Encoders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Option;

public class QueryStreamSpark implements Callable<Void> {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private SparkSession spark;
	private final int nStream;
	private final HashMap<Integer, String> queriesHT;
	private final int nQueries;
	private final Random random;
	private final ExecuteQueriesConcurrentSpark parent;

	
	public QueryStreamSpark(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			SparkSession spark, HashMap<Integer, String> queriesHT, int nQueries, Random random,
			ExecuteQueriesConcurrentSpark parent) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		//this.spark = spark;
		//In databricks, take the default session instead of using the same
		//session reference as in main.
		Option<SparkSession> opt = SparkSession.getDefaultSession();
		SparkSession session = opt.getOrElse(null);
		this.spark = session;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.random = random;
		this.parent = parent;
	}
	
	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in QueryStreamSpark useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	
	@Override
	public Void call() {
		this.logger.info("\n\n\n\n\nStarting query stream: " + this.nStream + " using " + this.parent.dbName);
		this.useDatabase(this.parent.dbName);
		this.spark.conf().set("spark.sql.crossJoin.enabled", "true");
		//this.logger.info(AppUtil.stringifySparkConfiguration(this.spark));
		//Integer[] queries = this.queriesHT.keySet().toArray(new Integer[] {});
		//Arrays.sort(queries);
		//this.shuffle(queries);
		int[] queries = StreamsTable.matrix[this.nStream];
		//int[] impalaKit = {19, 27, 3, 34, 42, 43, 46, 52, 53, 55, 59, 63, 65, 68, 7, 73, 79, 8, 82, 89, 98};
		//Arrays.sort(impalaKit);
		for(int i = 0; i < queries.length; i++) {
			//if( Arrays.binarySearch(impalaKit, queries[i]) < 0 )
			//	continue;
			String sqlStr = this.queriesHT.get(queries[i]);
			this.executeQuery(this.nStream, queries[i], sqlStr, i);
		}
		try {
			TimeUnit.SECONDS.sleep(5);
		}
		catch (InterruptedException ie) {
			ie.printStackTrace();
			this.logger.error("Error in QueryStreamSpark call.");
			this.logger.error(ie);
			this.logger.error(AppUtil.stringifyStackTrace(ie));
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
		String noExtFileName = "query" + nQuery;
		try {
			queryRecord = new QueryRecordConcurrent(nStream, nQuery, item);
			// Execute the query or queries.
			this.executeQueryMultipleCalls(nStream, noExtFileName, sqlStr, queryRecord, item);
			// Record the results file size.
			if( this.parent.saveResults ) {
				File resultsFile = new File(generateResultsFileName(noExtFileName, nStream, item));
				queryRecord.setResultsSize(resultsFile.length());
			}
			else
				queryRecord.setResultsSize(0);
			queryRecord.setSuccessful(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in the execution: " + noExtFileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.resultsQueue.add(queryRecord);
		}
	}
	
	
	private void executeQueryMultipleCalls(int nStream, String noExtFileName, String sqlStrFull,
			QueryRecordConcurrent queryRecord, int item) 
		throws Exception {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;
			this.spark.sparkContext().setJobDescription("Stream " + nStream + " item " + item + 
					" executing iteration " + iteration + " of query " + noExtFileName + ".");
			// Obtain the plan for the query.
			Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			//planDataset.write().mode(SaveMode.Append).csv(workDir + "/" + plansDir + "/" + 
			//noExtFileName);
			if( this.parent.savePlans )
				this.saveResults(generatePlansFileName(noExtFileName, nStream, item), planDataset, ! firstQuery);
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Stream " + nStream + " item " + item + 
					" executing iteration " + iteration + " of query " + noExtFileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			//dataset.write().mode(SaveMode.Append).csv(workDir + "/" + resultsDir + "/" + 
			//nStream + "_" + noExtFileName);
			if( this.parent.saveResults ) {
				int tuples = this.saveResults(generateResultsFileName(noExtFileName, nStream, item), dataset, ! firstQuery);
				queryRecord.setTuples(queryRecord.getTuples() + tuples);
			}
			firstQuery = false;
			iteration++;
		}
	}
	
	
	private int saveResults(String resFileName, Dataset<Row> dataset, boolean append) throws Exception {
		File temp = new File(resFileName);
		temp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(resFileName, append);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		// List<String> list = dataset.as(Encoders.STRING()).collectAsList();
		List<String> list = dataset.map(row -> row.mkString(" | "), Encoders.STRING()).collectAsList();
		for (String s : list)
			printWriter.println(s);
		printWriter.close();
		return list.size();
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


