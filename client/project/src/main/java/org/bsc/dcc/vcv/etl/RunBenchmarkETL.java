package org.bsc.dcc.vcv.etl;

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

public class RunBenchmarkETL {

	
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
	
	
	public RunBenchmarkETL(String args[]) throws Exception {
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
		RunBenchmarkETL application = null;
		try {
			application = new RunBenchmarkETL(args);
		}
		catch(Exception e) {
			System.exit(1);
		}
		application.runBenchmark(args);
	}
	
	
	private void runBenchmark(String[] args) {
		try {
			String scaleFactorStr = this.getParamValue(args, "scale-factor");
			long scaleFactor = Long.parseLong(scaleFactorStr);
			long newScaleFactor = 100;
			boolean doSchema = this.flags.charAt(0) == '1' ? true : false;
			if( doSchema ) {
				System.out.println("\n\n\nCreating the database schema.\n\n\n");
				CreateSchema.main(args);
				//Additional 100 gb database
				if( scaleFactor >= 1000 ) {
					String[] argsOneHundredSF = this.differentScaleFactorParams(args, scaleFactor,
							newScaleFactor);
					System.out.println("\n\n\nCreating the small database schema.\n\n\n");
					CreateSchema.main(argsOneHundredSF);
				}
			}
			boolean doLoad = this.flags.charAt(1) == '1' ? true : false;
			if( doLoad ) {
				this.saveTestParameters(args, "load");
				System.out.println("\n\n\nRunning the LOAD test.\n\n\n");
				CreateDatabase.main(args);
				//Additional 100 gb database
				if( scaleFactor >= 1000 ) {
					String[] argsOneHundredSF = this.differentScaleFactorParams(args, scaleFactor,
							newScaleFactor);
					Stream<String> argsStream = Arrays.stream(argsOneHundredSF)
							.filter(s -> ! s.contains("tpcds-test"));
					String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=loadsmall"), argsStream)
							.collect(Collectors.toList())
							.toArray(new String[0]);
					this.saveTestParameters(argsCopy, "loadsmall");
					System.out.println("\n\n\nRunning the LOAD small test.\n\n\n");
					CreateDatabase.main(argsCopy);
				}
			}
			boolean doAnalyze = this.flags.charAt(2) == '1' ? true : false;
			if( doAnalyze) {
				this.saveTestParameters(args, "analyze");
				System.out.println("\n\n\nRunning the ANALYZE test.\n\n\n");
				AnalyzeTables.main(args);
				//Additional 100 gb database
				if( scaleFactor >= 1000 ) {
					String[] argsOneHundredSF = this.differentScaleFactorParams(args, scaleFactor,
							newScaleFactor);
					Stream<String> argsStream = Arrays.stream(argsOneHundredSF)
							.filter(s -> ! s.contains("tpcds-test"));
					String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=analyzesmall"), argsStream)
							.collect(Collectors.toList())
							.toArray(new String[0]);
					this.saveTestParameters(argsCopy, "analyzesmall");
					System.out.println("\n\n\nRunning the ANALYZE small test.\n\n\n");
					AnalyzeTables.main(argsCopy);
				}
			}
			boolean doBillionInts = this.flags.charAt(3) == '1' ? true : false;
			if( doBillionInts ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=billionints"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "billionints");
				System.out.println("\n\n\nRunning the BILLION INTS test.\n\n\n");
				CreateDatabaseBillionIntsTest1.main(argsCopy);
			}
			boolean doWriteUnPartitioned = this.flags.charAt(4) == '1' ? true : false;
			if( doWriteUnPartitioned ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=writeunpartitioned"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "writeunpartitioned");
				System.out.println("\n\n\nRunning the WRITE UNPARTITIONED test.\n\n\n");
				CreateDatabaseWriteUnPartitionedTest2.main(argsCopy);
			}
			boolean doWritePartitioned = this.flags.charAt(5) == '1' ? true : false;
			if( doWritePartitioned ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=writepartitioned"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "writepartitioned");
				System.out.println("\n\n\nRunning the WRITE PARTITIONED test.\n\n\n");
				CreateDatabaseWritePartitionedTest3.main(argsCopy);
			}
			boolean doLoadDenorm = this.flags.charAt(6) == '1' ? true : false;
			if( doLoadDenorm ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=loaddenorm"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "loaddenorm");
				System.out.println("\n\n\nRunning the LOAD DENORM test.\n\n\n");
				CreateDatabaseDenormTest4.main(argsCopy);
				//Additional 100 gb database
				if( scaleFactor >= 1000 ) {
					String[] argsOneHundredSF = this.differentScaleFactorParams(args, scaleFactor,
							newScaleFactor);
					Stream<String> argsStream100 = Arrays.stream(argsOneHundredSF)
							.filter(s -> ! s.contains("tpcds-test"));
					String[] argsCopy100 = Stream.concat(Stream.of("--tpcds-test=loaddenormsmall"), 
							argsStream100)
							.collect(Collectors.toList())
							.toArray(new String[0]);
					this.saveTestParameters(argsCopy100, "loaddenormsmall");
					System.out.println("\n\n\nRunning the LOAD DENORM small test.\n\n\n");
					CreateDatabaseDenormTest4.main(argsCopy100);
				}
			}
			boolean doDenormDeepCopy = this.flags.charAt(7) == '1' ? true : false;
			if( doDenormDeepCopy ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=denormdeepcopy"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "denormdeepcopy");
				System.out.println("\n\n\nRunning the DENORM DEEP COPY test.\n\n\n");
				CreateDatabaseDeepCopyTest5.main(argsCopy);
			}
			boolean doThousandCols = this.flags.charAt(8) == '1' ? true : false;
			if( doThousandCols ) {
				/*
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=denormthousandcols"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "denormthousandcols");
				System.out.println("\n\n\nRunning the DENORM THOUSAND COLS test.\n\n\n");
				CreateDatabaseThousandColsTest6.main(argsCopy);
				*/
				//Additional 100 gb database
				if( scaleFactor >= 1000 ) {
					String[] argsOneHundredSF = this.differentScaleFactorParams(args, scaleFactor,
							newScaleFactor);
					Stream<String> argsStream100 = Arrays.stream(argsOneHundredSF)
							.filter(s -> ! s.contains("tpcds-test"));
					String[] argsCopy100 = Stream.concat(Stream.of("--tpcds-test=denormthousandcolssmall"), 
							argsStream100)
							.collect(Collectors.toList())
							.toArray(new String[0]);
					this.saveTestParameters(argsCopy100, "denormthousandcolssmall");
					System.out.println("\n\n\nRunning the DENORM THOUSAND COLS small test.\n\n\n");
					CreateDatabaseThousandColsTest6.main(argsCopy100);
				}
			}
			boolean doMerge = this.flags.charAt(9) == '1' ? true : false;
			if( doMerge ) {
				Stream<String> argsStream = Arrays.stream(args)
						.filter(s -> ! s.contains("tpcds-test"));
				String[] argsCopy = Stream.concat(Stream.of("--tpcds-test=denormmerge"), argsStream)
						.collect(Collectors.toList())
						.toArray(new String[0]);
				this.saveTestParameters(argsCopy, "denormmerge");
				System.out.println("\n\n\nRunning the DENORM MERGE test.\n\n\n");
				CreateDatabaseMergeTest7.main(argsCopy);
			}
			if( this.system.equals("sparkdatabricks")  ) {
				this.executeCommand("mkdir -p /dbfs/mnt/tpcds-results-test/" + this.resultsDir);
				this.executeCommand("cp -r " + this.workDir + "/" + this.resultsDir + "/* /dbfs/mnt/tpcds-results-test/" + this.resultsDir + "/");
			}
			else if( this.system.equals("sparkemr")  ) {
				this.executeCommand("aws s3 cp --recursive " + this.workDir + "/" + this.resultsDir + "/ " +
						"s3://tpcds-results-test/" + this.resultsDir + "/");
				this.executeCommand("aws s3 cp " + this.workDir + "/logs/all.log " +
						"s3://tpcds-results-test/" + this.resultsDir + "/logs/" + 
						this.experimentName + "/" + this.instance + "/all.log");
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
	
	
	private String[] differentScaleFactorParams(String[] args, long scaleFactor, long newScaleFactor) {
		String dbNameParamVal = this.getParamValue(args, "schema-name");
		dbNameParamVal = dbNameParamVal.replace("_" + scaleFactor + "gb_",
			"_" + newScaleFactor + "gb_");
		String[] argsCopy = this.replaceParamValue(args, "schema-name", dbNameParamVal);
		String extRawDataLocParamVal = this.getParamValue(args, "ext-raw-data-location");
		extRawDataLocParamVal = extRawDataLocParamVal.replace("" + scaleFactor, "" + newScaleFactor);
		argsCopy = this.replaceParamValue(argsCopy, "ext-raw-data-location", 
				extRawDataLocParamVal);
		String extTablesLocParamVal = this.getParamValue(args, "ext-tables-location");
		extTablesLocParamVal = extTablesLocParamVal.replace("-" + scaleFactor + "gb-",
			"-" + newScaleFactor + "gb-");
		argsCopy = this.replaceParamValue(argsCopy, "ext-tables-location", extTablesLocParamVal);
		return argsCopy;
	}
	
	
	private String getParamValue(String[] args, String paramNameSimplified) {
		String paramVal = null;
		Optional<String> paramKeyValPair = Arrays.stream(args)
				.filter(s -> s.contains("--" + paramNameSimplified)).findAny();
		if( paramKeyValPair.isPresent() ) {
			StringTokenizer tokenizer = new StringTokenizer(paramKeyValPair.get(), "=");
			if( tokenizer.hasMoreTokens() ) {
				String paramName = tokenizer.nextToken();
			}
			if( tokenizer.hasMoreTokens() )
				paramVal = tokenizer.nextToken();
		}
		return paramVal;
	}
	
	
	private String[] replaceParamValue(String[] args, String paramNameSimplified, String newParamValue) {
		Stream<String> argsStream = Arrays.stream(args)
				.filter(s -> ! s.contains("--" + paramNameSimplified));
		String[] argsCopy = Stream.concat(Stream.of("--" + paramNameSimplified + "=" + newParamValue),
				argsStream)
				.collect(Collectors.toList())
				.toArray(new String[0]);
		return argsCopy;
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
