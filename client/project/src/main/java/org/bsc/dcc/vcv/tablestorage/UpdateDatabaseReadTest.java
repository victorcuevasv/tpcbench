package org.bsc.dcc.vcv.tablestorage;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.sql.SQLException;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.etl.CreateDatabaseDenormETLTask;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.bsc.dcc.vcv.SkipMods;
import org.bsc.dcc.vcv.SkipKeys;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class UpdateDatabaseReadTest extends CreateDatabaseDenormETLTask {
	
	
	private final int readInstance;
	private final boolean saveResults;
	
	
	public UpdateDatabaseReadTest(CommandLine commandLine) {
		super(commandLine);
		String readInstanceStr = commandLine.getOptionValue("read-instance", "0");
		this.readInstance = Integer.parseInt(readInstanceStr);
		String saveResultsStr = commandLine.getOptionValue("save-power-results", "true");
		this.saveResults = Boolean.parseBoolean(saveResultsStr);
	}
	
	
	public static void main(String[] args) throws SQLException {
		UpdateDatabaseReadTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseReadTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseReadTest(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") || this.system.contains("databricks") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		JarCreateTableReaderAsZipFile readTestTableReader = new JarCreateTableReaderAsZipFile(
				this.jarFile, "ReadTestQueries");
		List<String> unorderedList = readTestTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQuery = readTestTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			readtestQuery(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void readtestQuery(String queryFileName, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			System.out.println("Processing query: " + index);
			this.logger.info("Processing query: " + index);
			String suffix = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				suffix = "delta";
			else
				suffix = "update";
			sqlQuery = sqlQuery.replace("<SUFFIX>", suffix);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			ResultSet rs = stmt.executeQuery(sqlQuery);
			if( this.saveResults ) {
				int tuples = this.saveResults(
						this.generateResultsFileName(queryFileName), rs, false);
				queryRecord.setTuples(tuples);
			}
			stmt.close();
			rs.close();
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseReadTest readtestQuery.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	

	private String generateResultsFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.resultsDir + "/readresults" +
				this.readInstance + "/" + this.experimentName + "/" + this.instance +
				"/" + noExtFileName + ".txt";
	}
	
	
	private int saveResults(String resFileName, ResultSet rs, boolean append) 
			throws Exception {
		File tmp = new File(resFileName);
		tmp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(resFileName, append);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		ResultSetMetaData metadata = rs.getMetaData();
		int nCols = metadata.getColumnCount();
		int tuples = 0;
		while (rs.next()) {
			StringBuilder rowBuilder = new StringBuilder();
			for (int i = 1; i <= nCols - 1; i++) {
				rowBuilder.append(rs.getString(i) + " | ");
			}
			rowBuilder.append(rs.getString(nCols));
			printWriter.println(rowBuilder.toString());
			tuples++;
		}
		printWriter.close();
		return tuples;
	}
	
	
}


