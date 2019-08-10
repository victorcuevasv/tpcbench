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


public class ExecuteQueriesSpark {
	
	//private static final Logger logger = LogManager.getLogger(ExecuteQueriesSpark.class);
	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private AnalyticsRecorder recorder;
	private JarQueriesReaderAsZipFile queriesReader;
	private String workDir;
	private String resultsDir;
	private String plansDir;
	private String system;
	private boolean savePlans;
	private boolean saveResults;
	private String dbName;
	private String test;
	private String queriesDir;
	private String folderName;
	private String experimentName;
	private int instance;

	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory to store the results
	 * args[2] subdirectory of work directory to store the execution plans
	 * args[3] system (system name used within the logs)
	 * args[4] save plans (boolean)
	 * args[5] save results (boolean)
	 * args[6] database name
	 * args[7] test (e.g. power)
	 * args[8] queries dir within the jar
	 * args[9] results folder name (e.g. for Google Drive)
	 * args[10] experiment name (name of subfolder within the results folder
	 * args[11] experiment instance number
	 * args[12] jar file
	 * args[13] "all" or query file
	 * 
	 */
	public ExecuteQueriesSpark(String[] args) {
		try {
			this.workDir = args[0];
			this.resultsDir = args[1];
			this.plansDir = args[2];
			this.system = args[3];
			this.savePlans = Boolean.parseBoolean(args[4]);
			this.saveResults = Boolean.parseBoolean(args[5]);
			this.dbName = args[6];
			this.test = args[7];
			this.queriesDir = args[8];
			this.folderName = args[9];
			this.experimentName = args[10];
			this.instance = Integer.parseInt(args[11]);
			this.queriesReader = new JarQueriesReaderAsZipFile(args[12], queriesDir);
			this.spark = SparkSession.builder().appName("TPC-DS Sequential Query Execution")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
			this.recorder = new AnalyticsRecorder(test, system, workDir, folderName, experimentName, instance);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}


	public static void main(String[] args) {
		if( args.length != 14 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Insufficient arguments: " + args.length);
			System.exit(1);
		}
		ExecuteQueriesSpark prog = new ExecuteQueriesSpark(args);
		String querySingleOrAllByNull = args[args.length-1].equalsIgnoreCase("all") ? null : args[args.length-1];
		prog.executeQueries(querySingleOrAllByNull);
	}
	
	public void executeQueries(String querySingleOrAllByNull) {
		this.spark.sql("USE " + dbName);
		this.recorder.header();
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			if( querySingleOrAllByNull != null ) {
				if( ! fileName.equals(querySingleOrAllByNull) )
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
		if( ! this.recorder.system.equals("sparkdatabricks") ) {
			this.spark.stop();
		}
	}
	
	
	private String generateResultsFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.folderName + "/" + this.resultsDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	private String generatePlansFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.folderName + "/" + this.plansDir + "/" + this.experimentName + 
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
			if( this.savePlans )
				planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			if( this.savePlans )
				this.saveResults(this.generatePlansFileName(queryFileName), planDataset, ! firstQuery);
			// Execute the query.
			System.out.println("Executing iteration " + iteration + " of query " + queryFileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			if( this.saveResults )
				this.saveResults(this.generateResultsFileName(queryFileName), dataset, ! firstQuery);
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
	
	private void saveResults(String resFileName, Dataset<Row> dataset, boolean append) {
		try {
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName, append);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			List<String> list = dataset.map(row -> row.mkString(" | "), Encoders.STRING()).collectAsList();
			for(String s: list)
				printWriter.println(s);
			printWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error saving results: " + resFileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}

