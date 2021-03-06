package org.bsc.dcc.vcv.tablestorage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bsc.dcc.vcv.AnalyzeTables;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.CreateDatabase;
import org.bsc.dcc.vcv.CreateSchema;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class RunTableStorageBenchmark {

	
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
	private final boolean forceCompaction;
	
	
	public RunTableStorageBenchmark(String args[]) throws Exception {
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkETL constructor.");
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
		String forceCompactionStr = commandLine.getOptionValue("hudi-mor-force-compaction", "false");
		this.forceCompaction = Boolean.parseBoolean(forceCompactionStr);
	}
	
	
	public static void main(String[] args) {
		RunTableStorageBenchmark application = null;
		try {
			application = new RunTableStorageBenchmark(args);
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
			boolean doAnalyze = this.flags.charAt(2) == '1' ? true : false;
			if( doAnalyze) {
				this.saveTestParameters(args, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTables.main(args);
			}
			boolean doLoadDenorm = this.flags.charAt(3) == '1' ? true : false;
			if( doLoadDenorm ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=loaddenorm"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "loaddenorm");
				System.out.println("\n\n\nRunning the LOAD DENORM test.\n\n\n");
				org.bsc.dcc.vcv.etl.CreateDatabaseDenormTest4.main(argsCopy);
			}
			boolean doLoadDenormSkip = this.flags.charAt(4) == '1' ? true : false;
			if( doLoadDenormSkip ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=loaddenormskip"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "loaddenormskip");
				System.out.println("\n\n\nRunning the LOAD DENORM SKIP test.\n\n\n");
				CreateDatabaseDenormSkip.main(argsCopy);
			}
			boolean doInsertData = this.flags.charAt(5) == '1' ? true : false;
			if( doInsertData ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=insertdata"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "insertdata");
				System.out.println("\n\n\nRunning the CREATE INSERT DATA test.\n\n\n");
				CreateDatabaseInsertData.main(argsCopy);
			}
			boolean doDeleteData = this.flags.charAt(6) == '1' ? true : false;
			if( doDeleteData ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=deletedata"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "deletedata");
				System.out.println("\n\n\nRunning the CREATE DELETE DATA test.\n\n\n");
				CreateDatabaseDeleteData.main(argsCopy);
			}
			boolean doUpdate = this.flags.charAt(7) == '1' ? true : false;
			if( doUpdate ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=loadupdate"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "loadupdate");
				System.out.println("\n\n\nRunning the CREATE LOAD UPDATE test.\n\n\n");
				CreateDatabaseDenormUpdate.main(argsCopy);
			}
			boolean doReadTest1 = this.flags.charAt(8) == '1' ? true : false;
			if( doReadTest1 ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				Stream<String> tempStream = Stream.concat(Stream.of("--tpcds-test=readtest1"), argsStream);
				String[] argsCopy = Stream.concat(tempStream, Stream.of("--read-instance=1"))
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "readtest1");
				System.out.println("\n\n\nRunning the READ test 1.\n\n\n");
				UpdateDatabaseReadTest.main(argsCopy);
			}
			boolean doInsUpdTest = this.flags.charAt(9) == '1' ? true : false;
			if( doInsUpdTest ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=insupdtest"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "insupdtest");
				System.out.println("\n\n\nRunning the INSERT/UPDATE test.\n\n\n");
				UpdateDatabaseInsUpdTest.main(argsCopy);
			}
			boolean doReadTest2 = this.flags.charAt(10) == '1' ? true : false;
			if( doReadTest2 ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				Stream<String> tempStream = Stream.concat(Stream.of("--tpcds-test=readtest2"), argsStream);
				String[] argsCopy = Stream.concat(tempStream, Stream.of("--read-instance=2"))
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "readtest2");
				System.out.println("\n\n\nRunning the READ test 2.\n\n\n");
				UpdateDatabaseReadTest.main(argsCopy);
			}
			boolean doDeleteTest = this.flags.charAt(11) == '1' ? true : false;
			if( doDeleteTest ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=deletetest"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "deletetest");
				System.out.println("\n\n\nRunning the DELETE test.\n\n\n");
				UpdateDatabaseDeleteTest.main(argsCopy);
			}
			boolean doReadTest3 = this.flags.charAt(12) == '1' ? true : false;
			if( doReadTest3 ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				Stream<String> tempStream = Stream.concat(Stream.of("--tpcds-test=readtest3"), argsStream);
				String[] argsCopy = Stream.concat(tempStream, Stream.of("--read-instance=3"))
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "readtest3");
				System.out.println("\n\n\nRunning the READ test 3.\n\n\n");
				UpdateDatabaseReadTest.main(argsCopy);
			}
			boolean doGdprTest = this.flags.charAt(13) == '1' ? true : false;
			if( doGdprTest ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=gdprtest"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "gdprtest");
				System.out.println("\n\n\nRunning the GDPR test.\n\n\n");
				UpdateDatabaseGdprTest.main(argsCopy);
			}
			boolean doReadTest4 = this.flags.charAt(14) == '1' ? true : false;
			if( doReadTest4 ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				Stream<String> tempStream = Stream.concat(Stream.of("--tpcds-test=readtest4"), argsStream);
				String[] argsCopy = Stream.concat(tempStream, Stream.of("--read-instance=4"))
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "readtest4");
				System.out.println("\n\n\nRunning the READ test 4.\n\n\n");
				UpdateDatabaseReadTest.main(argsCopy);
			}
			/*
			if( this.system.equals("sparkemr")  ) {
				this.executeCommand("aws s3 cp --recursive " + this.workDir + "/" + this.resultsDir + "/ " +
						"s3://tpcds-results-test/" + this.resultsDir + "/");
				this.executeCommand("aws s3 cp " + this.workDir + "/logs/all.log " +
						"s3://tpcds-results-test/" + this.resultsDir + "/logs/" + 
						this.experimentName + "/" + this.instance + "/all.log");
				//this.executeCommand("mkdir -p /mnt/tpcds-results-test/" + this.resultsDir);
				//this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
			*/
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in RunBenchmarkSpark runBenchmark.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
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
