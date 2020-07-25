package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class RunBenchmarkSparkUpdateCLI {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String workDir;
	private final String resultsDir;
	private final String experimentName;
	private final String instance;
	private final String system;
	private final String flags;
	private final Boolean analyze;
	private final String format;
	private final Boolean usePartitioning;
	private final CommandLine commandLine;
	
	
	public RunBenchmarkSparkUpdateCLI(String args[]) throws Exception {
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = this.commandLine.getOptionValue("main-work-dir");
		this.resultsDir = this.commandLine.getOptionValue("results-dir");
		this.experimentName = this.commandLine.getOptionValue("experiment-name");
		this.instance = this.commandLine.getOptionValue("instance-number");
		this.system = this.commandLine.getOptionValue("system-name");
		this.flags = this.commandLine.getOptionValue("execution-flags");
		this.analyze = Boolean.parseBoolean(this.commandLine.getOptionValue("use-row-stats"));
		this.format = this.commandLine.getOptionValue("table-format");
		this.usePartitioning = Boolean.parseBoolean(this.commandLine.getOptionValue("use-partitioning"));
	}
	
	
	public static void main(String[] args) {
		RunBenchmarkSparkUpdateCLI application = null;
		try {
			application = new RunBenchmarkSparkUpdateCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
		}
		application.runBenchmark(args);
	}
	
	
	private void runBenchmark(String[] args) {
		try {
			boolean doSchema = this.flags.charAt(0) == '1' ? true : false;
			if( doSchema ) {
				System.out.println("\n\n\nCreating the database schema.\n\n\n");
				CreateSchemaSpark.main(args);
			}
			boolean doLoad = this.flags.charAt(1) == '1' ? true : false;
			if( doLoad ) {
				this.saveTestParameters(args, "load");
				System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
				CreateDatabaseSpark.main(args);
			}
			boolean doLoadDenorm = this.flags.charAt(2) == '1' ? true : false;
			if( doLoadDenorm ) {
				this.saveTestParameters(args, "loaddenorm");
				System.out.println("\n\n\nRunning the LOAD DENORM test.\n\n\n");
				CreateDatabaseSparkDenorm.main(args);
			}
			boolean doLoadUpdate = this.flags.charAt(3) == '1' ? true : false;
			if( doLoadUpdate ) {
				this.saveTestParameters(args, "loadupdate");
				System.out.println("\n\n\nRunning the LOAD UPDATE test.\n\n\n");
				CreateDatabaseSparkUpdate.main(args);
			}
			//Redundant check for legacy compatibility.
			boolean doAnalyze = this.flags.charAt(4) == '1' ? true : false;
			if( this.analyze && doAnalyze) {
				this.saveTestParameters(args, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTablesSpark.main(args);
			}
			boolean doAnalyzeDenorm = this.flags.charAt(5) == '1' ? true : false;
			if( doAnalyzeDenorm ) {
				String[] executeQueriesAnalyzeDenormArgs =
						this.createExecuteQueriesAnalyzeDenormArgs(args);
				this.saveTestParameters(executeQueriesAnalyzeDenormArgs, "analyzedenorm");
				System.out.println("\n\n\nRunning the ANALYZE DENORM test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesAnalyzeDenormArgs);
			}
			boolean doAnalyzeUpdate = this.flags.charAt(6) == '1' ? true : false;
			if( doAnalyzeUpdate ) {
				String[] executeQueriesAnalyzeUpdateArgs =
						this.createExecuteQueriesAnalyzeUpdateArgs(args);
				this.saveTestParameters(executeQueriesAnalyzeUpdateArgs, "analyzeupdate");
				System.out.println("\n\n\nRunning the ANALYZE UPDATE test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesAnalyzeUpdateArgs);
			}
			boolean doZorder = this.flags.charAt(7) == '1' ? true : false;
			if( this.format.equalsIgnoreCase("delta") && doZorder ) {
				String[] executeQueriesSparkDeltaZorderArgs =
						this.createExecuteQueriesSparkDeltaZorderArgs(args);
				this.saveTestParameters(executeQueriesSparkDeltaZorderArgs, "zorder");
				System.out.println("\n\n\nRunning the Delta Z-ORDER test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesSparkDeltaZorderArgs);
			}
			boolean doZorderUpdate = this.flags.charAt(8) == '1' ? true : false;
			if( doZorderUpdate ) {
				String[] executeQueriesSparkDeltaZorderUpdateArgs =
						this.createExecuteQueriesSparkDeltaZorderUpdateArgs(args);
				this.saveTestParameters(executeQueriesSparkDeltaZorderUpdateArgs, "zorderupdate");
				System.out.println("\n\n\nRunning the Delta Z-ORDER UPDATE test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesSparkDeltaZorderUpdateArgs);
			}
			boolean doPower = this.flags.charAt(9) == '1' ? true : false;
			if( doPower ) {
				String[] executeQueriesSparkArgs =
						this.createExecuteQueriesSparkArgs(args);
				this.saveTestParameters(executeQueriesSparkArgs, "power");
				System.out.println("\n\n\nRunning the POWER test.\n\n\n");
				ExecuteQueriesSpark.main(executeQueriesSparkArgs);
			}
			boolean doTput = this.flags.charAt(10) == '1' ? true : false;
			if( doTput ) {
				this.saveTestParameters(args, "tput");
				System.out.println("\n\n\nRunning the TPUT test.\n\n\n");
				ExecuteQueriesConcurrentSpark.main(args);
			}
			if( this.system.equals("sparkdatabricks")  ) {
				this.executeCommand("mkdir -p /dbfs/mnt/tpcds-results-test/" + this.resultsDir);
				this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /dbfs/mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
			else if( this.system.equals("sparkemr")  ) {
				this.executeCommand("aws s3 cp --recursive " + this.workDir + "/" + this.resultsDir + "/ " +
						"s3://tpcds-results-test/" + this.resultsDir + "/");
				//this.executeCommand("mkdir -p /mnt/tpcds-results-test/" + this.resultsDir);
				//this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private String[] createExecuteQueriesSparkArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=power";
		array[array.length - 1] = "--queries-dir-in-jar=QueriesSpark";
		return array;
	}

	
	private String[] createExecuteQueriesSparkDeltaZorderArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=zorder";
		if( ! this.usePartitioning )
			array[array.length - 1] = "--queries-dir-in-jar=DatabricksDeltaZorderNoPart";
		else
			array[array.length - 1] = "--queries-dir-in-jar=DatabricksDeltaZorder";
		return array;
	}
	
	
	private String[] createExecuteQueriesSparkDeltaZorderUpdateArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=zorderupdate";
		array[array.length - 1] = "--queries-dir-in-jar=DatabricksDeltaZorderUpdate";
		return array;
	}
	
	
	private String[] createExecuteQueriesAnalyzeDenormArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=analyzedenorm";
		array[array.length - 1] = "--queries-dir-in-jar=AnalyzeDenorm";
		return array;
	}
	
	
	private String[] createExecuteQueriesAnalyzeUpdateArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=analyzeupdate";
		if( this.system.equals("sparkdatabricks") )
			array[array.length - 1] = "--queries-dir-in-jar=AnalyzeUpdateDelta";
		else if( this.system.equals("sparkemr") )
			array[array.length - 1] = "--queries-dir-in-jar=AnalyzeUpdateHudi";
		return array;
	}
	
	
	private void saveTestParameters(String args[], String test) {
		try {
			String paramsFileName = this.workDir + "/" + this.resultsDir + "/analytics/" + 
					this.experimentName + "/" + test + "/" + this.instance + "/parameters.log";
			File temp = new File(paramsFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(paramsFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			int counter = 0;
			for(int i = 0; i < args.length; i++) {
				printWriter.println(args[i]);
				if( (counter + 1) % 5 == 0 )
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

	private int executeCommand(String cmd) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("bash", "-c", cmd);
		int exitVal = -1;
		try {
			Process process = processBuilder.start();
			exitVal = process.waitFor();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
		}
		return exitVal;
	}

	
}
