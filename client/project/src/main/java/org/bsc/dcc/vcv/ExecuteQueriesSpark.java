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
	private final AnalyticsRecorder recorder;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final String workDir;
	private final String dbName;
	private final String folderName;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String queriesDir;
	private final String resultsDir;
	private final String plansDir;
	private final boolean savePlans;
	private final boolean saveResults;
	private final String jarFile;

	
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
	 * args[13] "all" or query file
	 * 
	 */
	public ExecuteQueriesSpark(String[] args) {
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
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.folderName, this.experimentName,
				this.system, this.test, this.instance);
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Sequential Query Execution")
				.config("spark.sql.crossJoin.enabled", "true")
				//.config("spark.sql.shuffle.partitions", "200")
				.enableHiveSupport()
				.getOrCreate();
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
			logger.error("Incorrect number of arguments: " + args.length);
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
		this.recorder.close();
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
			if( this.saveResults ) {
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

