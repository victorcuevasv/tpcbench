package org.bsc.dcc.vcv;

import java.io.FileWriter;
import java.io.IOException;
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


public class ExecuteQueriesSpark {
	
	private static final Logger logger = LogManager.getLogger(ExecuteQueriesSpark.class);
	private SparkSession spark;
	private AnalyticsRecorder recorder;
	private JarQueriesReaderAsZipFile queriesReader;

	public ExecuteQueriesSpark(String jarFile) {
		this.queriesReader = new JarQueriesReaderAsZipFile(jarFile);
		this.spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("spark.sql.crossJoin.enabled", "true")
				.enableHiveSupport()
				.getOrCreate();
		this.recorder = new AnalyticsRecorder();
	}

	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory to store the results
	 * args[2] subdirectory of work directory to store the execution plans
	 * args[3] jar file
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) {
		ExecuteQueriesSpark prog = new ExecuteQueriesSpark(args[3]);
		prog.executeQueries(args[0], args[1], args[2]);
	}
	
	public void executeQueries(String workDir, String resultsDir, String plansDir) {
		this.recorder.header();
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			String sqlStr = this.queriesReader.getFile(fileName);
			String nQueryStr = fileName.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			QueryRecord queryRecord = new QueryRecord(nQuery);
			//if( ! fileName.equals("query64.sql") )
			//	continue;
			System.out.println("\n\n\n\n\n---------------------------------------------------------");
			System.out.println(sqlStr);
			System.out.println("\n\n\n\n\n---------------------------------------------------------");
			try {
				this.executeQueryMultipleCalls(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
				String noExtFileName = fileName.substring(0, fileName.indexOf('.'));
				long resultsSize = calculateSize(workDir + "/" + resultsDir + "/" + noExtFileName, 
						".csv", this.logger);
				queryRecord.setResultsSize(resultsSize);
				queryRecord.setSuccessful(true);
			}
			catch(Exception e) {
				e.printStackTrace();
				this.logger.error("Error processing: " + fileName);
				this.logger.error(e);
			}
			finally {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
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
			// Obtain the plan for the query.
			Dataset<Row> planDataset = this.spark.sql("EXPLAIN " + sqlStr);
			String noExtFileName = fileName.substring(0, fileName.indexOf('.'));
			planDataset.write().mode(SaveMode.Overwrite).csv(workDir + "/" + plansDir + "/" + noExtFileName);
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + fileName + ".");
			Dataset<Row> dataset = this.spark.sql(sqlStr);
			// Save the results.
			if( firstQuery )
				dataset.write().mode(SaveMode.Overwrite).csv(workDir + "/" + resultsDir + "/" + noExtFileName);
			else
				dataset.write().mode(SaveMode.Append).csv(workDir + "/" + resultsDir + "/" + noExtFileName);
			firstQuery = false;
			iteration++;
		}
	}
	
	/*
	 * Calculate the size of the files ending with a given extension and stored in a given folder.
	 * 
	 * Since the operation is non-atomic, the returned value may be inaccurate.
	 * However, this method is quick and does its best.
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

}

