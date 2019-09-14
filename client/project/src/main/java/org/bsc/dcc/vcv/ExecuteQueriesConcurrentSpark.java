package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.sql.DriverManager;
import java.io.*;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecuteQueriesConcurrentSpark implements ConcurrentExecutor {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final AnalyticsRecorderConcurrent recorder;
	private final ExecutorService executor;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final JarQueriesReaderAsZipFile queriesReader;
	private static final int POOL_SIZE = 100;
	private final Random random;
	final String workDir;
	final String dbName;
	final String folderName;
	final String experimentName;
	final String system;
	final String test;
	final int instance;
	final String queriesDir;
	final String resultsDir;
	final String plansDir;
	final boolean savePlans;
	final boolean saveResults;
	final private String jarFile;
	final private int nStreams;
	final private long seed;
	
	
	/**
	 * @param args
	 *  
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * 
	 * args[5] test name (e.g. power)
	 * args[6] experiment instance number
	 * args[7] queries dir within the jar
	 * args[8] subdirectory of work directory to store the results
	 * args[9] subdirectory of work directory to store the execution plans
	 * 
	 * args[10] save plans (boolean)
	 * args[11] save results (boolean)
	 * args[12] jar file
	 * args[13] number of streams
	 * args[14] random seed
	 * 
	 */
	public ExecuteQueriesConcurrentSpark(String[] args) {
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
		this.jarFile = args[12];
		this.nStreams = Integer.parseInt(args[13]);
		this.seed = Long.parseLong(args[14]);
		this.random = new Random(seed);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.folderName,
				this.experimentName, this.system, this.test, this.instance);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Throughput Test")
				.config("spark.sql.crossJoin.enabled", "true")
				.config("spark.sql.shuffle.partitions", "200")
				.enableHiveSupport()
				.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}


	public static void main(String[] args) {
		if( args.length != 15 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		ExecuteQueriesConcurrentSpark prog = new ExecuteQueriesConcurrentSpark(args);
		prog.executeStreams(false);
	}
	
	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	public void executeStreams(boolean singleCall) {
		List<String> files = queriesReader.getFilesOrdered();
		HashMap<Integer, String> queriesHT = createQueriesHT(files, this.queriesReader);
		int nQueries = files.size();
		this.useDatabase(dbName);
		int totalQueries = nQueries * nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < nStreams; i++) {
			QueryStreamSpark stream = new QueryStreamSpark(i, this.resultsQueue, this.spark,
					queriesHT, nQueries, random, this);
			this.executor.submit(stream);
		}
		this.executor.shutdown();
		try {
			this.executor.awaitTermination(7L, TimeUnit.DAYS);
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark executeStreams.");
			this.logger.error(ie);
			this.logger.error(AppUtil.stringifyStackTrace(ie));
		}
	}
	
	
	public HashMap<Integer, String> createQueriesHT(List<String> files, JarQueriesReaderAsZipFile queriesReader) {
		HashMap<Integer, String> queriesHT = new HashMap<Integer, String>();
		for(String file : files) {
			int nQuery = ExecuteQueriesConcurrentSpark.extractNumber(file);
			String sqlStr = queriesReader.getFile(file);
			queriesHT.put(nQuery, sqlStr);
		}
		return queriesHT;
	}
	
	
	// Converts a string representing a filename like query12.sql to the integer 12.
	public static int extractNumber(String fileName) {
		String nStr = fileName.substring(0, fileName.indexOf('.')).replaceAll("[^\\d.]", "");
		return Integer.parseInt(nStr);
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

}


