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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class RunBenchmarkCLI {
	
	protected static final Logger logger = LogManager.getLogger("AllLog");
	private final String workDir;
	private final String resultsDir;
	private final String experimentName;
	private final String instance;
	private final String system;
	private final String flags;
	private final Boolean analyze;
	private final String format;
	private final Boolean usePartitioning;
	
	public RunBenchmarkCLI(String args[]) throws Exception {
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.instance = commandLine.getOptionValue("instance-number");
		this.system = commandLine.getOptionValue("system-name");
		this.flags = commandLine.getOptionValue("execution-flags");
		this.analyze = Boolean.parseBoolean(commandLine.getOptionValue("use-row-stats"));
		this.format = commandLine.getOptionValue("table-format");
		this.usePartitioning = Boolean.parseBoolean(commandLine.getOptionValue("use-partitioning"));
	}
	
	public static void main(String[] args) {
		RunBenchmarkCLI application = null;
		try {
			application = new RunBenchmarkCLI(args);
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
				CreateSchema.main(args);
			}
			boolean doLoad = this.flags.charAt(1) == '1' ? true : false;
			if( doLoad ) {
				this.saveTestParameters(args, "load");
				System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
				CreateDatabase.main(args);
			}
			//Redundant check for legacy compatibility.
			boolean doAnalyze = this.flags.charAt(2) == '1' ? true : false;
			if( this.analyze && doAnalyze) {
				this.saveTestParameters(args, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTables.main(args);
			}
			boolean doZorder = this.flags.charAt(3) == '1' ? true : false;
			if( this.format.equalsIgnoreCase("delta") && doZorder ) {
				String[] executeQueriesDeltaZorderArgs =
						this.createExecuteQueriesDeltaZorderArgs(args);
				this.saveTestParameters(executeQueriesDeltaZorderArgs, "zorder");
				System.out.println("\n\n\nRunning the Delta Z-ORDER test.\n\n\n");
				ExecuteQueries.main(executeQueriesDeltaZorderArgs);
			}
			boolean doPower = this.flags.charAt(4) == '1' ? true : false;
			if( doPower ) {
				String[] executeQueriesArgs =
						this.createExecuteQueriesArgs(args);
				this.saveTestParameters(executeQueriesArgs, "power");
				System.out.println("\n\n\nRunning the POWER test.\n\n\n");
				ExecuteQueries.main(executeQueriesArgs);
			}
			boolean doTput = this.flags.charAt(5) == '1' ? true : false;
			if( doTput ) {
				this.saveTestParameters(args, "tput");
				System.out.println("\n\n\nRunning the TPUT test.\n\n\n");
				ExecuteQueriesConcurrent.main(args);
			}
			if( this.system.equals("sparkdatabricks")  ) {
				this.executeCommand("mkdir -p /dbfs/mnt/tpcds-results-test/" + this.resultsDir);
				this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /dbfs/mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
			else if( this.system.equals("sparkemr") || this.system.equals("prestoemr")  ) {
				this.executeCommand("aws s3 cp --recursive " + this.workDir + "/" + this.resultsDir + "/ " +
						"s3://tpcds-results-test/" + this.resultsDir + "/");
				this.executeCommand("echo hello > $HOME/hello.txt");
				this.executeCommand("mkdir -p /mnt/tpcds-results-test/" + this.resultsDir);
				this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private String[] createExecuteQueriesArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=power";
		array[array.length - 1] = "--queries-dir-in-jar=QueriesSpark";
		return array;
	}
	
	private String[] createExecuteQueriesDeltaZorderArgs(String args[]) {
		String[] array = new String[args.length + 2];
		System.arraycopy(args, 0, array, 0, args.length);
		array[array.length - 2] = "--tpcds-test=zorder";
		if( ! this.usePartitioning )
			array[array.length - 1] = "--queries-dir-in-jar=DatabricksDeltaZorderNoPart";
		else
			array[array.length - 1] = "--queries-dir-in-jar=DatabricksDeltaZorder";
		return array;
	}
	
	protected void saveTestParameters(String args[], String test) {
		try {
			String paramsFileName = this.workDir + "/" + this.resultsDir + "/analytics/" + 
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
		processBuilder.redirectErrorStream(true);
		processBuilder.command("bash", "-c", cmd);
		StringBuilder builder = new StringBuilder();
		try {
			Process process = processBuilder.start();
			BufferedReader input = new BufferedReader(new 
				     InputStreamReader(process.getInputStream()));
			String s = null;
			while ((s = input.readLine()) != null) {
			    builder.append(s);
			}
			int exitVal = process.waitFor();
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
