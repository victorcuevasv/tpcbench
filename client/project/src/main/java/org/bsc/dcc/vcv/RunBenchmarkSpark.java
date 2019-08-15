package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RunBenchmarkSpark {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String workDir;
	private final String folderName;
	private final String experimentName;
	private final String instance;
	
	
	public RunBenchmarkSpark(String args[]) {
		this.workDir = args[0];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.instance = args[5];
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
	 * args[5] experiment instance number
	 * args[6] directory for generated data raw files
	 * args[7] subdirectory within the jar that contains the create table files
	 * args[8] suffix used for intermediate table text files
	 * args[9] prefix of external location for raw data tables (e.g. S3 bucket), null for none
	 * 
	 * args[10] prefix of external location for created tables (e.g. S3 bucket), null for none
	 * args[11] format for column-storage tables (PARQUET, DELTA)
	 * args[12] whether to run queries to count the tuples generated (true/false)
	 * args[13] jar file
	 * args[14] queries dir within the jar
	 * 
	 * args[15] subdirectory of work directory to store the results
	 * args[16] subdirectory of work directory to store the execution plans
	 * args[17] save power test plans (boolean)
	 * args[18] save power test results (boolean)
	 * args[19] "all" or query file
	 * 
	 * args[20] save tput test plans (boolean)
	 * args[21] save tput test results (boolean)
	 * args[22] number of streams
	 * args[23] random seed
	 * 
	 */
	public static void main(String[] args) {
		if( args.length != 24 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		RunBenchmarkSpark application = new RunBenchmarkSpark(args);
		application.runBenchmark(args);
	}
	
	
	public void runBenchmark(String[] args) {
		String[] createDatabaseSparkArgs = this.createCreateDatabaseSparkArgs(args);
		String[] executeQueriesSparkArgs = this.createExecuteQueriesSparkArgs(args);
		String[] executeQueriesConcurrentSparkArgs = this.createExecuteQueriesConcurrentSparkArgs(args);
		try {
			this.saveTestParameters(createDatabaseSparkArgs, "load");
			CreateDatabaseSpark.main(createDatabaseSparkArgs);
			TimeUnit.SECONDS.sleep(10);
			this.saveTestParameters(executeQueriesSparkArgs, "power");
			ExecuteQueriesSpark.main(executeQueriesSparkArgs);
			TimeUnit.SECONDS.sleep(10);
			this.saveTestParameters(executeQueriesConcurrentSparkArgs, "tput");
			ExecuteQueriesConcurrentSpark.main(executeQueriesConcurrentSparkArgs);
			TimeUnit.SECONDS.sleep(10);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private String[] createCreateDatabaseSparkArgs(String args[]) {
		/* 
		args[0] main work directory
		args[1] schema (database) name
		args[2] results folder name (e.g. for Google Drive)
		args[3] experiment name (name of subfolder within the results folder)
		args[4] system name (system name used within the logs)
		
		args[5] test name (i.e. load)
		args[6] experiment instance number
		args[7] directory for generated data raw files
		args[8] subdirectory within the jar that contains the create table files
		args[9] suffix used for intermediate table text files
		
		args[10] prefix of external location for raw data tables (e.g. S3 bucket), null for none
		args[11] prefix of external location for created tables (e.g. S3 bucket), null for none
		args[12] format for column-storage tables (PARQUET, DELTA)
		args[13] whether to run queries to count the tuples generated (true/false)
		args[14] jar file
		*/
		String[] array = new String[15];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "load";
		array[6] = args[5];
		array[7] = args[6];
		array[8] = args[7];
		array[9] = args[8];
		
		array[10] = args[9];
		array[11] = args[10];
		array[12] = args[11];
		array[13] = args[12];
		array[14] = args[13];
		
		return array;
	}
	
	
	private String[] createExecuteQueriesSparkArgs(String args[]) {
		/* 
		args[0] main work directory
		args[1] schema (database) name
		args[2] results folder name (e.g. for Google Drive)
		args[3] experiment name (name of subfolder within the results folder)
		args[4] system name (system name used within the logs)
		
		args[5] test name (e.g. power)
		args[6] experiment instance number
		args[7] queries dir within the jar
		args[8] subdirectory of work directory to store the results
		args[9] subdirectory of work directory to store the execution plans
		
		args[10] save plans (boolean)
		args[11] save results (boolean)
		args[12] jar file
		args[13] "all" or query file
		*/
		String[] array = new String[14];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "power";
		array[6] = args[5];
		array[7] = args[14];
		array[8] = args[15];
		array[9] = args[16];
		
		array[10] = args[17];
		array[11] = args[18];
		array[12] = args[13];
		array[13] = args[19];
		
		return array;
	}
	
	
	private String[] createExecuteQueriesConcurrentSparkArgs(String args[]) {
		/* 
		args[0] main work directory
		args[1] schema (database) name
		args[2] results folder name (e.g. for Google Drive)
		args[3] experiment name (name of subfolder within the results folder)
		args[4] system name (system name used within the logs)
		
		args[5] test name (e.g. power)
		args[6] experiment instance number
		args[7] queries dir within the jar
		args[8] subdirectory of work directory to store the results
		args[9] subdirectory of work directory to store the execution plans

		args[10] save plans (boolean)
		args[11] save results (boolean)
		args[12] jar file
		args[13] number of streams
		args[14] random seed
		*/
		String[] array = new String[15];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "tput";
		array[6] = args[5];
		array[7] = args[14];
		array[8] = args[15];
		array[9] = args[16];
		
		array[10] = args[20];
		array[11] = args[21];
		array[12] = args[13];
		array[13] = args[22];
		array[14] = args[23];
		
		return array;
	}
	
	
	public void saveTestParameters(String args[], String test) {
		try {
			String paramsFileName = this.workDir + "/" + this.folderName + "/analytics/" + 
					this.experimentName + "/" + test + "/" + this.instance + "/parameters.log";
			File temp = new File(paramsFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(paramsFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			int counter = 0;
			for(int i = 0; i < args.length; i++) {
				printWriter.println("args[" + i + "] = " + args[i]);
				if( counter % 5 == 0 )
					printWriter.println();
				counter++;
			}
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}

	
}
