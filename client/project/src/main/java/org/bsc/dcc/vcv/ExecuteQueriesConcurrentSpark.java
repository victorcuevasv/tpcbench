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
	private AnalyticsRecorderConcurrent recorder;
	private ExecutorService executor;
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private JarQueriesReaderAsZipFile queriesReader;
	private static final int POOL_SIZE = 100;
	private long seed;
	private Random random;
	boolean savePlans;
	boolean saveResults;

	public ExecuteQueriesConcurrentSpark(String jarFile, String system, 
			boolean savePlans, boolean saveResults) {
		this.savePlans = savePlans;
		this.saveResults = saveResults;
		this.queriesReader = new JarQueriesReaderAsZipFile(jarFile, "QueriesSpark");
		this.spark = SparkSession.builder().appName("TPC-DS Throughput Test")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		this.recorder = new AnalyticsRecorderConcurrent("tput", system);
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
	 * args[4] system (directory name used to store logs)
	 * args[5] number of streams
	 * args[6] random seed
	 * args[7] save plans (boolean)
	 * args[8] save results (boolean)
	 * args[9] database name
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) {
		if( args.length != 10 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		boolean savePlans = Boolean.parseBoolean(args[7]);
		boolean saveResults = Boolean.parseBoolean(args[8]);
		ExecuteQueriesConcurrentSpark prog = new ExecuteQueriesConcurrentSpark(args[3], args[4],
				savePlans, saveResults);
		List<String> files = prog.queriesReader.getFilesOrdered();
		HashMap<Integer, String> queriesHT = prog.createQueriesHT(files, prog.queriesReader);
		int nStreams = Integer.parseInt(args[5]);
		long seed = Long.parseLong(args[6]);
		prog.seed = seed;
		prog.random = new Random(seed);
		int nQueries = files.size();
		prog.executeStreams(nQueries, nStreams, prog.random, queriesHT,
				args[0], args[1], args[2], false, args[9]);
	}
	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark copyLog.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private void copyLog(String logFile, String duplicateFile) {
		try {
			BufferedReader inBR = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
			File tmp = new File(duplicateFile);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(duplicateFile);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			String line = null;
			while ((line = inBR.readLine()) != null) {
				printWriter.println(line);
			}
			inBR.close();
			printWriter.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSpark copyLog.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	public void executeStreams(int nQueries, int nStreams, Random random, HashMap<Integer, String> queriesHT,
			String workDir, String resultsDir, String plansDir, boolean singleCall, String dbName) {
		this.useDatabase(dbName);
		int totalQueries = nQueries * nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < nStreams; i++) {
			QueryStreamSpark stream = new QueryStreamSpark(i, this.resultsQueue, this.spark,
					queriesHT, nQueries, workDir, resultsDir, plansDir, singleCall, random, 
					this.recorder.system, this.savePlans, this.saveResults, dbName);
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
		//In the case of Spark on Databricks, copy the /data/logs/analytics.log file to
		// /dbfs/data/logs/tput/sparkdatabricks/analyticsDuplicate.log, in case the application is
		//running on a job cluster that will be shutdown automatically after completion.
		if( this.recorder.system.equals("sparkdatabricks") ) {
			this.copyLog("/data/logs/analytics.log",
				"/dbfs/data/logs/tput/sparkdatabricks/analyticsDuplicate.log");
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
		this.spark.stop();
	}

}


