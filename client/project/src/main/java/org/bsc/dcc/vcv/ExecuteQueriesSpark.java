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
	boolean savePlans;
	boolean saveResults;

	public ExecuteQueriesSpark(String jarFile, String system, boolean savePlans, boolean saveResults) {
		try {
			this.savePlans = savePlans;
			this.saveResults = saveResults;
			this.queriesReader = new JarQueriesReaderAsZipFile(jarFile, "QueriesSpark");
			this.spark = SparkSession.builder().appName("TPC-DS Sequential Query Execution")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
			this.recorder = new AnalyticsRecorder("power", system);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory to store the results
	 * args[2] subdirectory of work directory to store the execution plans
	 * args[3] jar file
	 * args[4] system (directory name used to store logs)
	 * args[5] save plans (boolean)
	 * args[6] save results (boolean)
	 * args[7] database name
	 * args[8] "all" or query file
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) {
		if( args.length != 9 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		boolean savePlans = Boolean.parseBoolean(args[5]);
		boolean saveResults = Boolean.parseBoolean(args[6]);
		ExecuteQueriesSpark prog = new ExecuteQueriesSpark(args[3], args[4], savePlans, saveResults);
		String queryFile = args[args.length-1].equalsIgnoreCase("all") ? null : args[args.length-1];
		prog.executeQueries(args[0], args[1], args[2], queryFile, args[7]);
	}
	
	public void executeQueries(String workDir, String resultsDir, String plansDir,
			String queryFile, String dbName) {
		this.spark.sql("USE " + dbName);
		this.recorder.header();
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			String sqlStr = this.queriesReader.getFile(fileName);
			String nQueryStr = fileName.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			QueryRecord queryRecord = new QueryRecord(nQuery);
			if( queryFile != null ) {
				if( ! fileName.equals(queryFile) )
					continue;
			}
			this.logger.info("\nExecuting query: " + fileName + "\n" + sqlStr);
			try {
				this.executeQueryMultipleCalls(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
				String noExtFileName = fileName.substring(0, fileName.indexOf('.'));
				//long resultsSize = calculateSize(workDir + "/" + resultsDir + "/" + noExtFileName, ".csv", this.logger);
				//queryRecord.setResultsSize(resultsSize);
				File resultsFile = new File(workDir + "/" + resultsDir + "/" + "power" + "/" + 
						this.recorder.system + "/" + noExtFileName + ".txt");
				queryRecord.setResultsSize(resultsFile.length());
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
		//In the case of Spark on Databricks, copy the /data/logs/analytics.log file to
		// /dbfs/data/logs/tput/sparkdatabricks/analyticsDuplicate.log, in case the application is
		//running on a job cluster that will be shutdown automatically after completion.
		if( this.recorder.system.equals("sparkdatabricks") ) {
			this.copyLog("/data/logs/analytics.log",
					"/dbfs/data/logs/power/sparkdatabricks/analyticsDuplicate.log");
		}
		this.spark.stop();
	}
	
	// Execute a query from the provided file.
	private void executeQuerySingleCall(String workDir, String resultsDir, String plansDir, 
			String fileName, String sqlStr, QueryRecord queryRecord) {
		// Remove the last semicolon.
		sqlStr = sqlStr.trim();
		sqlStr = sqlStr.substring(0, sqlStr.length() - 1);
		// Obtain the plan for the query.
		Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
		String noExtFileName = fileName.substring(0, fileName.indexOf('.'));
		planDataset.write().mode(SaveMode.Overwrite).csv(workDir + "/" + plansDir + "/" + noExtFileName);
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		Dataset<Row> dataset = this.spark.sql(sqlStr);
		// Save the results.
		dataset.write().mode(SaveMode.Overwrite).csv(workDir + "/" + resultsDir + "/" + noExtFileName);
	}
	
	private void executeQueryMultipleCalls(String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStrFull, QueryRecord queryRecord) {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;
			String noExtFileName = fileName.substring(0, fileName.indexOf('.'));
			this.spark.sparkContext().setJobDescription("Executing iteration " + iteration + 
					" of query " + fileName + ".");
			// Obtain the plan for the query.
			Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			//planDataset.write().mode(SaveMode.Overwrite).csv(workDir + "/" + plansDir + "/" + noExtFileName);
			if( this.savePlans )
				this.saveResults(workDir + "/" + plansDir + "/" + "power" + "/" + 
						this.recorder.system + "/" + noExtFileName + ".txt", planDataset, ! firstQuery);
			// Execute the query.
			System.out.println("Executing iteration " + iteration + " of query " + fileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			//dataset.write().mode(SaveMode.Append).csv(workDir + "/" + resultsDir + "/" + noExtFileName);
			if( this.saveResults )
				this.saveResults(workDir + "/" + resultsDir + "/" + "power" + "/" + 
						this.recorder.system + "/" + noExtFileName + ".txt", dataset, ! firstQuery);
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
			//List<String> list = dataset.as(Encoders.STRING()).collectAsList();
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

}

