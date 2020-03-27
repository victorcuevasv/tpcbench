package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RunBenchmarkSpark {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String workDir;
	private final String folderName;
	private final String experimentName;
	private final String instance;
	private final String system;
	
	
	public RunBenchmarkSpark(String args[]) {
		this.workDir = args[0];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.instance = args[5];
		this.system = args[4];
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
	 * args[14] jar file
	 * 
	 * args[15] whether to generate statistics by analyzing tables (true/false)
	 * args[16] if argument above is true, whether to compute statistics for columns (true/false)
	 * args[17] queries dir within the jar
	 * args[18] subdirectory of work directory to store the results
	 * args[19] subdirectory of work directory to store the execution plans
	 * 
	 * args[20] save power test plans (boolean)
	 * args[21] save power test results (boolean)
	 * args[22] "all" or query file
	 * args[23] save tput test plans (boolean)
	 * args[24] save tput test results (boolean)
	 * 
	 * args[25] number of streams
	 * args[26] random seed
	 * args[27] flags (111111 schema|load|analyze|zorder|power|tput)
	 * 
	 */
	public static void main(String[] args) {
		if( args.length != 28 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		RunBenchmarkSpark application = new RunBenchmarkSpark(args);
		application.runBenchmark(args);
	}
	
	
	private void runBenchmark(String[] args) {
		String[] createSchemaSparkArgs = this.createCreateSchemaSparkArgs(args);
		String[] createDatabaseSparkArgs = this.createCreateDatabaseSparkArgs(args);
		String[] executeQueriesSparkArgs = this.createExecuteQueriesSparkArgs(args);
		String[] executeQueriesConcurrentSparkArgs = this.createExecuteQueriesConcurrentSparkArgs(args);
		try {
			boolean doSchema = args[27].charAt(0) == '1' ? true : false;
			if( doSchema ) {
				System.out.println("\n\n\nCreating the database schema.\n\n\n");
				CreateSchemaSpark.main(createSchemaSparkArgs);
			}
			boolean doLoad = args[27].charAt(1) == '1' ? true : false;
			if( doLoad ) {
				this.saveTestParameters(createDatabaseSparkArgs, "load");
				System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
				CreateDatabaseSpark.main(createDatabaseSparkArgs);
			}
			boolean analyze = Boolean.parseBoolean(args[15]);
			//Redundant check for legacy compatibility.
			boolean doAnalyze = args[27].charAt(2) == '1' ? true : false;
			if( analyze && doAnalyze) {
				String[] analyzeTablesSparkArgs = this.createAnalyzeTablesSparkArgs(args);
				this.saveTestParameters(analyzeTablesSparkArgs, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTablesSpark.main(analyzeTablesSparkArgs);
			}
			boolean doZorder = args[27].charAt(3) == '1' ? true : false;
			if( args[11].equalsIgnoreCase("delta") && doZorder ) {
				boolean noPart = args[13].equals("false");
				String[] executeQueriesSparkDeltaZorderArgs =
						this.createExecuteQueriesSparkDeltaZorderArgs(args, noPart);
				this.saveTestParameters(executeQueriesSparkDeltaZorderArgs, "zorder");
				System.out.println("\n\n\nRunning the Delta Z-ORDER test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesSparkDeltaZorderArgs);
			}
			boolean doPower = args[27].charAt(4) == '1' ? true : false;
			if( doPower ) {
				this.saveTestParameters(executeQueriesSparkArgs, "power");
				System.out.println("\n\n\nRunning the POWER test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesSparkArgs);
			}
			boolean doTput = args[27].charAt(5) == '1' ? true : false;
			if( doTput ) {
				this.saveTestParameters(executeQueriesConcurrentSparkArgs, "tput");
				System.out.println("\n\n\nRunning the TPUT test.\n\n\n");
				ExecuteQueriesConcurrentSpark.main(executeQueriesConcurrentSparkArgs);
			}
			if( this.system.equals("sparkdatabricks")  ) {
				//this.executeCommand("mkdir /dbfs" + args[0] + "/" + this.instance);
				//this.executeCommand("cp -r " + args[0] + " /dbfs" + args[0] + "/" + this.instance);
				this.executeCommand("cp -r " + args[0] + "/" + args[2] + "/* /dbfs/mnt/" + args[2] + "/");
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

	private String[] createCreateSchemaSparkArgs(String args[]) {
		/* 
		args[0] system used to create the schema on the metastore
		args[1] schema (database) name
		*/
		String[] array = new String[2];
		array[0] = args[4];
		array[1] = args[1];
		
		return array;
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
		args[14] whether to use data partitioning for the tables (true/false)
		
		args[15] jar file
		*/
		String[] array = new String[16];
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
		
		array[15]= args[14];
		
		return array;
	}
	
	
	private String[] createAnalyzeTablesSparkArgs(String args[]) {
		/* 
		args[0] main work directory
		args[1] schema (database) name
		args[2] results folder name (e.g. for Google Drive)
		args[3] experiment name (name of subfolder within the results folder)
		args[4] system name (system name used within the logs)
		args[5] test name (i.e. analyze)
		args[6] experiment instance number
		args[7] compute statistics for columns (true/false)
		*/
		String[] array = new String[8];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "analyze";
		array[6] = args[5];
		array[7] = args[16];
		
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
		array[7] = args[17];
		array[8] = args[18];
		array[9] = args[19];
		
		array[10] = args[20];
		array[11] = args[21];
		array[12] = args[14];
		array[13] = args[22];
		
		return array;
	}
	
	
	private String[] createExecuteQueriesSparkDeltaZorderArgs(String args[], boolean noPart) {
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
		
		array[5] = "zorder";
		array[6] = args[5];
		if( noPart )
			array[7] = "DatabricksDeltaZorderNoPart";
		else
			array[7] = "DatabricksDeltaZorder";
		array[8] = args[18];
		array[9] = args[19];
		
		array[10] = "false";
		array[11] = "false";
		array[12] = args[14];
		array[13] = "all";
		
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
		array[7] = args[17];
		array[8] = args[18];
		array[9] = args[19];
		
		array[10] = args[23];
		array[11] = args[24];
		array[12] = args[14];
		array[13] = args[25];
		array[14] = args[26];
		
		return array;
	}
	
	
	private void saveTestParameters(String args[], String test) {
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

	private void executeCommand(String cmd) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("bash", "-c", cmd);
		try {
			Process process = processBuilder.start();
			int exitVal = process.waitFor();
			System.out.println(exitVal);
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
		}
	}

	
}
