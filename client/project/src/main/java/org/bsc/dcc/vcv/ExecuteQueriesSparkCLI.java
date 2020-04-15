package org.bsc.dcc.vcv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Encoders;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class ExecuteQueriesSparkCLI {
	
	//private static final Logger logger = LogManager.getLogger(ExecuteQueriesSpark.class);
	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final AnalyticsRecorder recorder;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String queriesDir;
	private final String resultsSubDir;
	private final String plansSubDir;
	private final boolean savePlans;
	private final boolean saveResults;
	private final String jarFile;
	private final String querySingleOrAll;
	

	public ExecuteQueriesSparkCLI(String[] args) throws Exception {
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
			this.spark = SparkSession.builder().appName("TPC-DS Sequential Query Execution")
				.enableHiveSupport()
				.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = this.commandLine.getOptionValue("main-work-dir");
		this.dbName = this.commandLine.getOptionValue("schema-name");
		this.resultsDir = this.commandLine.getOptionValue("results-dir");
		this.experimentName = this.commandLine.getOptionValue("experiment-name");
		this.system = this.commandLine.getOptionValue("system-name");
		this.test = this.commandLine.getOptionValue("tpcds-test", "power");
		String instanceStr = this.commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.queriesDir = this.commandLine.getOptionValue("queries-dir-in-jar", "QueriesSpark");
		this.resultsSubDir = this.commandLine.getOptionValue("results-subdir");
		this.plansSubDir = this.commandLine.getOptionValue("plans-subdir");
		String savePlansStr = this.commandLine.getOptionValue("save-power-plans");
		this.savePlans = Boolean.parseBoolean(savePlansStr);
		String saveResultsStr = this.commandLine.getOptionValue("save-power-results");
		this.saveResults = Boolean.parseBoolean(saveResults);
		this.jarFile = this.commandLine.getOptionValue("jar-file");
		this.querySingleOrAll = this.commandLine.getOptionValue("all-or-query-file");
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
	}


	public static void main(String[] args) {
		
		ExecuteQueriesSparkCLI application = null;
		try {
			application = new ExecuteQueriesSparkCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
		}
		application.executeQueries();
	}
	
	public void executeQueries() {
		this.spark.sql("USE " + dbName);
		this.recorder.header();
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			if( ! this.querySingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.querySingleOrAll) )
					continue;
			}
			String sqlStr = this.queriesReader.getFile(fileName);
			String nQueryStr = fileName.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			QueryRecord queryRecord = new QueryRecord(nQuery);
			this.logger.info("\nExecuting query: " + fileName + "\n" + sqlStr);
			try {
				this.executeQueryMultipleCalls(fileName, sqlStr, queryRecord);
				if( this.saveResults ) {
					String queryResultsFileName = this.generateResultsFileName(fileName);
					File resultsFile = new File(queryResultsFileName);
					queryRecord.setResultsSize(resultsFile.length());
				}
				else
					queryRecord.setResultsSize(0);
				queryRecord.setSuccessful(true);
			}
			catch(Exception e) {
				e.printStackTrace();
				this.logger.error("Error processing: " + fileName);
				this.logger.error(e);
				this.logger.error(AppUtil.stringifyStackTrace(e));
			}
			finally {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
		this.recorder.close();
	}
	
	
	private String generateResultsFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.resultsDir + "/" + this.resultsSubDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	private String generatePlansFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.resultsDir + "/" + this.plansSubDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	private void executeQueryMultipleCalls(String queryFileName, String sqlStrFull, QueryRecord queryRecord) {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;
			this.spark.sparkContext().setJobDescription("Executing iteration " + iteration + 
					" of query " + queryFileName + ".");
			// Obtain the plan for the query.
			Dataset<Row> planDataset = null;
			if( this.test.equals("power") && this.savePlans )
				planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			if( this.test.equals("power") && this.savePlans )
				this.saveResults(this.generatePlansFileName(queryFileName), planDataset, ! firstQuery);
			// Execute the query.
			System.out.println("Executing iteration " + iteration + " of query " + queryFileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			if( this.test.equals("power") && this.saveResults ) {
				int tuples = this.saveResults(this.generateResultsFileName(queryFileName), dataset, ! firstQuery);
				queryRecord.setTuples(queryRecord.getTuples() + tuples);
			}
			firstQuery = false;
			iteration++;
		}
	}
	
	/*
	 * Calculate the size of the files ending with a given extension and stored in a given folder.
	 * 
	 * Since the operation is non-atomic, the returned value may be inaccurate.
	 */
	public static long calculateSize(String pathStr, String extension, Logger logger) {

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
	
	private int saveResults(String resFileName, Dataset<Row> dataset, boolean append) {
		int tuples = 0;
		try {
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName, append);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			List<String> list = dataset.map(row -> row.mkString(" | "), Encoders.STRING()).collectAsList();
			for(String s: list)
				printWriter.println(s);
			printWriter.close();
			tuples = list.size();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in saving results: " + resFileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return tuples;
	}
	

}

