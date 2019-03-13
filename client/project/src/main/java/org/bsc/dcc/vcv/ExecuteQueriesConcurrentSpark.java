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
import org.apache.spark.sql.SparkSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecuteQueriesConcurrentSpark implements ConcurrentExecutor {

	private static final Logger logger = LogManager.getLogger(ExecuteQueriesConcurrentSpark.class);
	private SparkSession spark;
	private AnalyticsRecorderConcurrent recorder;
	private ExecutorService executor;
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private JarQueriesReaderAsZipFile queriesReader;
	private static final int POOL_SIZE = 100;
	private long seed;
	private Random random;

	public ExecuteQueriesConcurrentSpark(String jarFile) {
		this.queriesReader = new JarQueriesReaderAsZipFile(jarFile);
		this.spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		this.recorder = new AnalyticsRecorderConcurrent("tput", "system");
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory to store the results
	 * args[2] subdirectory of work directory to store the execution plans
	 * args[3] jar file
	 * args[4] number of streams
	 * args[5] random seed
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws SQLException {
		ExecuteQueriesConcurrentSpark prog = new ExecuteQueriesConcurrentSpark(args[3]);
		List<String> files = prog.queriesReader.getFilesOrdered();
		HashMap<Integer, String> queriesHT = prog.createQueriesHT(files, prog.queriesReader);
		int nStreams = Integer.parseInt(args[4]);
		long seed = Long.parseLong(args[5]);
		prog.seed = seed;
		prog.random = new Random(seed);
		int nQueries = files.size();
		prog.executeStreams(nQueries, nStreams, prog.random, queriesHT,
				args[0], args[1], args[2], false);
	}
	
	public void executeStreams(int nQueries, int nStreams, Random random, HashMap<Integer, String> queriesHT,
			String workDir, String resultsDir, String plansDir, boolean singleCall) {
		int totalQueries = nQueries * nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 1; i <= nStreams; i++) {
			QueryStreamSpark stream = new QueryStreamSpark(i, this.resultsQueue, this.spark,
					queriesHT, nQueries, workDir, resultsDir, plansDir, singleCall, random);
			this.executor.submit(stream);
		}
		this.executor.shutdown();
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
		this.spark.stop();
	}

}


