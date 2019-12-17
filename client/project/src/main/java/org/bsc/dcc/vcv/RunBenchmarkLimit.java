package org.bsc.dcc.vcv;


import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RunBenchmarkLimit extends RunBenchmark {
	
	
	public RunBenchmarkLimit(String args[]) {
		super(args);
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
	 * args[13] whether to use data partitioning for the tables (true/false)
	 * args[14] hostname of the server
	 * 
	 * args[15] username for the connection
	 * args[16] jar file
	 * args[17] whether to generate statistics by analyzing tables (true/false)
	 * args[18]	if argument above is true, whether to compute statistics for columns (true/false)
	 * args[19] queries dir within the jar
	 * 
	 * args[20] subdirectory of work directory to store the results
	 * args[21] subdirectory of work directory to store the execution plans
	 * args[22] save power test plans (boolean)
	 * args[23] save power test results (boolean)
	 * args[24] "all" or query file
	 * 
	 * args[25] save tput test plans (boolean)
	 * args[26] save tput test results (boolean)
	 * args[27] number of streams
	 * args[28] random seed
	 * args[29] number of workers
	 * args[30] use multiple connections (true|false)
	 * 
	 */
	public static void main(String[] args) {
		if( args.length != 31 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		RunBenchmarkLimit application = new RunBenchmarkLimit(args);
		application.runBenchmark(args);
	}
	
	
	private void runBenchmark(String[] args) {
		String[] createDatabaseArgs = this.createCreateDatabaseArgs(args);
		String[] executeQueriesArgs = this.createExecuteQueriesArgs(args);
		String[] executeQueriesConcurrentLimitArgs = this.createExecuteQueriesConcurrentLimitArgs(args);
		try {
			this.saveTestParameters(createDatabaseArgs, "load");
			System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
			CreateDatabase.main(createDatabaseArgs);
			TimeUnit.SECONDS.sleep(10);
			boolean analyze = Boolean.parseBoolean(args[17]);
			if( analyze ) {
				String[] analyzeTablesArgs = this.createAnalyzeTablesArgs(args);
				this.saveTestParameters(analyzeTablesArgs, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTables.main(analyzeTablesArgs);
				TimeUnit.SECONDS.sleep(10);
			}
			if( args[11].equalsIgnoreCase("delta") ) {
				String[] executeQueriesDeltaZorderArgs = this.createExecuteQueriesDeltaZorderArgs(args);
				this.saveTestParameters(executeQueriesDeltaZorderArgs, "zorder");
				System.out.println("\n\n\nRunning the Delta Z-ORDER test.\n\n\n");
				ExecuteQueries.main(executeQueriesDeltaZorderArgs);
			}
			this.saveTestParameters(executeQueriesArgs, "power");
			System.out.println("\n\n\nRunning the POWER test.\n\n\n");
			ExecuteQueries.main(executeQueriesArgs);
			TimeUnit.SECONDS.sleep(10);
			this.saveTestParameters(executeQueriesConcurrentLimitArgs, "tput");
			System.out.println("\n\n\nRunning the TPUT test.\n\n\n");
			ExecuteQueriesConcurrentLimit.main(executeQueriesConcurrentLimitArgs);
			TimeUnit.SECONDS.sleep(10);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	protected String[] createExecuteQueriesConcurrentLimitArgs(String args[]) {
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
		args[12] hostname of the server
		args[13] jar file
		args[14] number of streams
		
		args[15] random seed
		args[16] number of workers
		args[17] use multiple connections (true|false)
		*/
		String[] array = new String[18];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "tput";
		array[6] = args[5];
		array[7] = args[19];
		array[8] = args[20];
		array[9] = args[21];
		
		array[10] = args[25];
		array[11] = args[26];
		array[12] = args[14];
		array[13] = args[16];
		array[14] = args[27];
		
		array[15] = args[28];
		array[16] = args[29];
		array[17] = args[30];
		
		return array;
	}

	
}


