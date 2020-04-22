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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

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
	final String resultsDir;
	final String experimentName;
	final String system;
	final String test;
	final int instance;
	final String queriesDir;
	final String resultsSubDir;
	final String plansSubDir;
	final boolean savePlans;
	final boolean saveResults;
	final private String jarFile;
	final private int nStreams;
	final private long seed;
	
	public ExecuteQueriesConcurrentSpark(CommandLine commandLine) {
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Throughput Test")
				.enableHiveSupport()
				.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "tput");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.queriesDir = commandLine.getOptionValue("queries-dir-in-jar", "QueriesSpark");
		this.resultsSubDir = commandLine.getOptionValue("results-subdir", "results");
		this.plansSubDir = commandLine.getOptionValue("plans-subdir", "plans");
		String savePlansStr = commandLine.getOptionValue("save-tput-plans", "false");
		this.savePlans = Boolean.parseBoolean(savePlansStr);
		String saveResultsStr = commandLine.getOptionValue("save-tput-results", "true");
		this.saveResults = Boolean.parseBoolean(saveResultsStr);
		this.jarFile = commandLine.getOptionValue("jar-file");
		String nStreamsStr = commandLine.getOptionValue("number-of-streams"); 
		this.nStreams = Integer.parseInt(nStreamsStr);
		String seedStr = commandLine.getOptionValue("random-seed", "1954"); 
		this.seed = Long.parseLong(seedStr);
		this.random = new Random(seed);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.resultsDir,
				this.experimentName, this.system, this.test, this.instance);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
	}
	
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
		if( args.length != 15 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		this.workDir = args[0];
		this.dbName = args[1];
		this.resultsDir = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.queriesDir = args[7];
		this.resultsSubDir = args[8];
		this.plansSubDir = args[9];
		this.savePlans = Boolean.parseBoolean(args[10]);
		this.saveResults = Boolean.parseBoolean(args[11]);
		this.jarFile = args[12];
		this.nStreams = Integer.parseInt(args[13]);
		this.seed = Long.parseLong(args[14]);
		this.random = new Random(seed);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.resultsDir,
				this.experimentName, this.system, this.test, this.instance);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Throughput Test")
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
		ExecuteQueriesConcurrentSpark application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new ExecuteQueriesConcurrentSpark(args);
		else {
			CommandLine commandLine = null;
			try {
				RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
				Options options = runOptions.getOptions();
				CommandLineParser parser = new DefaultParser();
				commandLine = parser.parse(options, args);
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error("Error in ExecuteQueriesConcurrentSpark main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new ExecuteQueriesConcurrentSpark(commandLine);
		}
		application.executeStreams();
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
	
	public void executeStreams() {
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
	
	public void saveSnowflakeHistory() {
		
	}
	
	public void incrementAtomicCounter() {
		
	}

	
}


