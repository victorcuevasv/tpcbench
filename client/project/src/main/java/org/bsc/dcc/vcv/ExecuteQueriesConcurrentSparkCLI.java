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


public class ExecuteQueriesConcurrentSparkCLI implements ConcurrentExecutor {

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
	private final CommandLine commandLine;
	
	public ExecuteQueriesConcurrentSparkCLI(String[] args) throws Exception {
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
			this.spark = SparkSession.builder().appName("TPC-DS Throughput Test")
				.enableHiveSupport()
				.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrentSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = this.commandLine.getOptionValue("main-work-dir");
		this.dbName = this.commandLine.getOptionValue("schema-name");
		this.resultsDir = this.commandLine.getOptionValue("results-dir");
		this.experimentName = this.commandLine.getOptionValue("experiment-name");
		this.system = this.commandLine.getOptionValue("system-name");
		this.test = this.commandLine.getOptionValue("tpcds-test", "tput");
		String instanceStr = this.commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.queriesDir = this.commandLine.getOptionValue("queries-dir-in-jar", "QueriesSpark");
		this.resultsSubDir = this.commandLine.getOptionValue("results-subdir", "results");
		this.plansSubDir = this.commandLine.getOptionValue("plans-subdir", "plans");
		String savePlansStr = this.commandLine.getOptionValue("save-tput-plans", "false");
		this.savePlans = Boolean.parseBoolean(savePlansStr);
		String saveResultsStr = this.commandLine.getOptionValue("save-tput-results", "true");
		this.saveResults = Boolean.parseBoolean(saveResultsStr);
		this.jarFile = this.commandLine.getOptionValue("jar-file");
		String nStreamsStr = this.commandLine.getOptionValue("number-of-streams"); 
		this.nStreams = Integer.parseInt(nStreamsStr);
		String seedStr = this.commandLine.getOptionValue("random-seed", "1954"); 
		this.seed = Long.parseLong(seedStr);
		this.random = new Random(seed);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.resultsDir,
				this.experimentName, this.system, this.test, this.instance);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
	}


	public static void main(String[] args) {
		ExecuteQueriesConcurrentSparkCLI application = null;
		try {
			application = new ExecuteQueriesConcurrentSparkCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
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
			QueryStreamSparkCLI stream = new QueryStreamSparkCLI(i, this.resultsQueue, this.spark,
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
			int nQuery = ExecuteQueriesConcurrentSparkCLI.extractNumber(file);
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


