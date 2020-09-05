package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Optional;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class UpdateDatabaseSparkForceCompaction {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	private final JarCreateTableReaderAsZipFile createTableReader;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String createTableDir;
	private final Optional<String> extTablePrefixCreated;
	private final String format;
	private final boolean doCount;
	private final boolean partition;
	private final String jarFile;
	private final String createSingleOrAll;
	private final String denormSingleOrAll;
	private final Map<String, String> precombineKeys;
	private final Map<String, String> primaryKeys;
	private final String customerSK;
	private final HudiUtil hudiUtil;
	private final String hudiFileSize;
	private final boolean hudiUseMergeOnRead;
	private final boolean defaultCompaction;
	private final int compactInstance;
	
	public UpdateDatabaseSparkForceCompaction(CommandLine commandLine) {
		try {

			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
					.enableHiveSupport()
					.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		String compactInstanceStr = commandLine.getOptionValue("compact-instance", "0");
		this.compactInstance = Integer.parseInt(compactInstanceStr);
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		//this.test = commandLine.getOptionValue("tpcds-test", "loadupdate");
		this.test = "compacttest" + this.compactInstance;
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.createTableDir = "QueriesHudiCompact";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		//this.format = commandLine.getOptionValue("table-format");
		if( this.system.equalsIgnoreCase("sparkdatabricks") )
			this.format = "delta";
		else
			this.format = "hudi";
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.denormSingleOrAll = commandLine.getOptionValue("denorm-all-or-file", "all");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.primaryKeys = new HudiPrimaryKeys().getMap();
		this.customerSK = commandLine.getOptionValue("gdpr-customer-sk");
		this.hudiFileSize = commandLine.getOptionValue("hudi-file-max-size", "1073741824");
		String hudiUseMergeOnReadStr = commandLine.getOptionValue("hudi-merge-on-read", "true");
		this.hudiUseMergeOnRead = Boolean.parseBoolean(hudiUseMergeOnReadStr);
		this.defaultCompaction = true;
		this.hudiUtil = new HudiUtil(this.dbName, this.workDir, this.resultsDir, 
				this.experimentName, this.instance, this.hudiFileSize, this.hudiUseMergeOnRead,
				this.defaultCompaction);
	}
	

	public static void main(String[] args) throws SQLException {
		UpdateDatabaseSparkForceCompaction application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseSparkForceCompaction main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseSparkForceCompaction(commandLine);
		application.compactTables();
	}
	
	
	private void compactTables() {
		// Process each .sql create table file found in the jar file.
		this.useDatabase(this.dbName);
		this.recorder.header();
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQuery = this.createTableReader.getFile(fileName);
			if( fileName.startsWith("avroSchema") ) {
				String fileNameNoExt = fileName.substring(0, fileName.indexOf('.'));
				this.saveFile(this.workDir + "/avroschema/" + fileNameNoExt + ".json", sqlQuery);
				this.executeCommand("aws s3 cp " + this.workDir + "/avroschema/" + fileNameNoExt +
						".json " + this.extTablePrefixCreated.get() + "/" + fileName);
				continue;
			}
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			if( this.format.equals("delta") ) {
				;
			}
			else if( this.format.equals("hudi") ) {
				forceCompactTableHudi(fileName, i);
			}
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}

	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void forceCompactTableHudi(String sqlFilename, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlFilename.substring(0, sqlFilename.indexOf('.'));
			String denormTableName = tableName + "_denorm";
			String denormHudiTableName = tableName + "_denorm_hudi";
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			int compactionInstant = this.scheduleCompaction(denormHudiTableName);
			this.runCompaction(denormHudiTableName, compactionInstant);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction forceCompactTableHudi.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private int scheduleCompaction(String denormHudiTableName) {
		int retVal = -1;
		String cmd = "(echo \"connect --path " +
			this.extTablePrefixCreated.get() + "/" + denormHudiTableName + "\" ; echo \"compaction schedule\") " +
			"| /usr/lib/hudi/cli/bin/hudi-cli.sh";
		System.out.println("Schedule compaction command : \n" + cmd);
		this.logger.info("Schedule compaction command : \n" + cmd);
		retVal = this.executeCommandCompaction(cmd);
		return retVal;
	}
	
	
	private int runCompaction(String denormHudiTableName, int compactionInstant) {
		int retVal = -1;
		String cmd = "(echo \"connect --path " +
			this.extTablePrefixCreated.get() + "/" + denormHudiTableName + "\" ; echo " + 
			"\"compaction run --parallelism 20 --schemaFilePath " +
			this.extTablePrefixCreated.get() + "/avroSchema_store_sales.json --sparkMemory  18971M --retry 1 " +
			"--compactionInstant " + compactionInstant + ")" +
			"| /usr/lib/hudi/cli/bin/hudi-cli.sh";
		System.out.println("Run compaction command : \n" + cmd);
		this.logger.info("Run compaction command : \n" + cmd);
		retVal = this.executeCommandCompaction(cmd);
		return retVal;
	}
	
	
	private int executeCommandCompaction(String cmd) {
		ProcessBuilder processBuilder = new ProcessBuilder();
		processBuilder.command("bash", "-c", cmd);
		StringBuilder builder = new StringBuilder();
		int retVal = -1;
		try {
			Process process = processBuilder.start();
			int exitVal = process.waitFor();
			BufferedReader input = new BufferedReader(new 
				     InputStreamReader(process.getInputStream()));
			String line = null;
			while ((line = input.readLine()) != null) {
			    if( line.contains("Compaction successfully completed for") ) {
			    	retVal = this.extractCommitTime(line);
			    }
			    builder.append(line + "\n");
			}
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		catch(InterruptedException ie) {
			ie.printStackTrace();
		}
		System.out.println("Compaction command output: \n" + builder.toString());
		this.logger.info("Compaction command output: \n" + builder.toString());
		return retVal;
	}
	
	
	private int extractCommitTime(String line) {
		int retVal = -1;
		StringTokenizer tokenizer = new StringTokenizer(line);
		while( tokenizer.hasMoreTokens() ) {
			String token = tokenizer.nextToken();
			if( token.length() < 14 )
				continue;
			boolean isNumber = true;
			for(int i = 0; i < token.length(); i++) {
				if( ! Character.isDigit(token.charAt(i)) ) {
					isNumber = false;
					break;
				}
			}
			if( isNumber ) {
				retVal = Integer.parseInt(token);
				break;
			}
		}
		return retVal;
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
	
	
	private void saveFile(String fileName, String contents) {
		try {
			File tmp = new File(fileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(fileName, false);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(contents);
			printWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in saving avro schema: " + fileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseSparkForceCompaction closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


