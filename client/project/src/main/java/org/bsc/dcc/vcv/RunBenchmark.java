package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RunBenchmark {

	
	protected static final Logger logger = LogManager.getLogger("AllLog");
	private final String workDir;
	private final String folderName;
	private final String experimentName;
	private final String instance;
	private String hostname;
	
	
	public RunBenchmark(String args[]) {
		this.workDir = args[0];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.instance = args[5];
		//Execute the unix command to obtain the hostname if so specified.
		//Usually it is enough to use localhost instead.
		if( args[14].equals("$(hostname)") || args[14].equals("$(hostname -f)") ) {
			args[14] = executeCommand("hostname -f");
		}
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
	 * args[29] use multiple connections (true|false)
	 * 
	 * args[30] flags (111111 schema|load|analyze|zorder|power|tput)
	 * 
	 */
	public static void main(String[] args) {
		if( args.length != 31 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		RunBenchmark application = new RunBenchmark(args);
		application.runBenchmark(args);
	}
	
	
	private void runBenchmark(String[] args) {
		String[] createSchemaArgs = this.createCreateSchemaArgs(args);
		String[] createDatabaseArgs = this.createCreateDatabaseArgs(args);
		String[] executeQueriesArgs = this.createExecuteQueriesArgs(args);
		String[] executeQueriesConcurrentArgs = this.createExecuteQueriesConcurrentArgs(args);
		try {
			boolean doSchema = args[30].charAt(0) == '1' ? true : false;
			if( doSchema ) {
				System.out.println("\n\n\nCreating the database schema.\n\n\n");
				CreateSchema.main(createSchemaArgs);
			}
			boolean doLoad = args[30].charAt(1) == '1' ? true : false;
			if( doLoad ) {
				this.saveTestParameters(createDatabaseArgs, "load");
				System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
				CreateDatabase.main(createDatabaseArgs);
			}
			boolean analyze = Boolean.parseBoolean(args[17]);
			//Redundant check for legacy compatibility.
			boolean doAnalyze = args[30].charAt(2) == '1' ? true : false;
			if( analyze && doAnalyze) {
				String[] analyzeTablesArgs = this.createAnalyzeTablesArgs(args);
				this.saveTestParameters(analyzeTablesArgs, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTables.main(analyzeTablesArgs);
			}
			boolean doZorder = args[30].charAt(3) == '1' ? true : false;
			if( args[11].equalsIgnoreCase("delta") && doZorder ) {
				boolean noPart = args[13].equals("false");
				String[] executeQueriesDeltaZorderArgs = 
						this.createExecuteQueriesDeltaZorderArgs(args, noPart);
				this.saveTestParameters(executeQueriesDeltaZorderArgs, "zorder");
				System.out.println("\n\n\nRunning the Delta Z-ORDER test.\n\n\n");
				ExecuteQueries.main(executeQueriesDeltaZorderArgs);
			}
			boolean doPower = args[30].charAt(4) == '1' ? true : false;
			if( doPower ) {
				this.saveTestParameters(executeQueriesArgs, "power");
				System.out.println("\n\n\nRunning the POWER test.\n\n\n");
				ExecuteQueries.main(executeQueriesArgs);
			}
			boolean doTput = args[30].charAt(5) == '1' ? true : false;
			if( doTput ) {
				this.saveTestParameters(executeQueriesConcurrentArgs, "tput");
				System.out.println("\n\n\nRunning the TPUT test.\n\n\n");
				ExecuteQueriesConcurrent.main(executeQueriesConcurrentArgs);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private String[] createCreateSchemaArgs(String args[]) {
		/* 
		 args[0] hostname of the server
		 args[1] system used to create the schema on the metastore
		 args[2] schema (database) name
		*/
		String[] array = new String[3];
		array[0] = args[14];
		array[1] = args[4];
		array[2] = args[1];
		
		return array;
	}
	
	
	protected String[] createCreateDatabaseArgs(String args[]) {
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
		
		args[15] whether to use bucketing for Hive and Presto
		args[16] hostname of the server
		args[17] username for the connection
		args[18] jar file
		*/
		String[] array = new String[19];
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
		
		array[15] = "true";
		array[16] = args[14];
		array[17] = args[15];
		array[18] = args[16];
		
		return array;
	}
	
	
	protected String[] createAnalyzeTablesArgs(String args[]) {
		/* 
		args[0] main work directory
		args[1] schema (database) name
		args[2] results folder name (e.g. for Google Drive)
		args[3] experiment name (name of subfolder within the results folder)
		args[4] system name (system name used within the logs)
		args[5] test name (i.e. load)
		args[6] experiment instance number
		args[7] compute statistics for columns (true/false)
		args[8] hostname of the server
		*/
		String[] array = new String[9];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "analyze";
		array[6] = args[5];
		array[7] = args[18];
		array[8] = args[14];
		
		return array;
	}
	
	
	protected String[] createExecuteQueriesArgs(String args[]) {
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
		args[14] "all" or query file
		*/
		String[] array = new String[15];
		array[0] = args[0];
		array[1] = args[1];
		array[2] = args[2];
		array[3] = args[3];
		array[4] = args[4];
		
		array[5] = "power";
		array[6] = args[5];
		array[7] = args[19];
		array[8] = args[20];
		array[9] = args[21];
		
		array[10] = args[22];
		array[11] = args[23];
		array[12] = args[14];
		array[13] = args[16];
		array[14] = args[24];
		
		return array;
	}
	
	
	protected String[] createExecuteQueriesDeltaZorderArgs(String args[], boolean noPart) {
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
		args[14] "all" or query file
		*/
		String[] array = new String[15];
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
		array[8] = args[20];
		array[9] = args[21];
		
		array[10] = "false";
		array[11] = "false";
		array[12] = args[14];
		array[13] = args[16];
		array[14] = "all";
		
		return array;
	}
	
	
	protected String[] createExecuteQueriesConcurrentArgs(String args[]) {
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
		args[16] use multiple connections (true|false)
		*/
		String[] array = new String[17];
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
		
		return array;
	}
	
	
	protected void saveTestParameters(String args[], String test) {
		try {
			String paramsFileName = this.workDir + "/" + this.folderName + "/analytics/" + 
					this.experimentName + "/" + test + "/" + this.instance + "/parameters.log";
			File temp = new File(paramsFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(paramsFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			int counter = 1;
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
	
	
	private String executeCommand(String cmd) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("bash", "-c", cmd);
		StringBuilder builder = new StringBuilder();
		try {
			Process process = processBuilder.start();
			int exitVal = process.waitFor();
			BufferedReader input = new BufferedReader(new 
				     InputStreamReader(process.getInputStream()));
			String s = null;
			while ((s = input.readLine()) != null) {
			    builder.append(s);
			}
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
		}
		return builder.toString();
	}

	
}
