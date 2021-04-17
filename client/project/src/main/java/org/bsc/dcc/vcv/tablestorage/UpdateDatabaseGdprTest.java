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
import java.sql.Connection;
import java.sql.ResultSet;
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


public class UpdateDatabaseGdprTest extends CreateDatabaseDenormETLTask {
	
	
	public UpdateDatabaseGdprTest(CommandLine commandLine) {
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		UpdateDatabaseGdprTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseGdprTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseGdprTest(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") || this.system.contains("databricks") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		JarCreateTableReaderAsZipFile gdprTestTableReader = new JarCreateTableReaderAsZipFile(
				this.jarFile, "DatabricksDeltaGdpr");
		List<String> unorderedList = gdprTestTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlMerge = gdprTestTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			gdprtest(fileName, sqlMerge, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void gdprtest(String sqlMergeFilename, String sqlMerge, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlMergeFilename.substring(0, sqlMergeFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String suffix = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				suffix = "delta";
			else
				suffix = "update";
			String denormUpdateTableName = tableNameRoot + "_denorm_" + suffix;
			sqlMerge = sqlMerge.replace("<FORMAT>", suffix);
			sqlMerge = sqlMerge.replace("<CUSTOMER_SK>", this.customerSK);
			saveCreateTableFile("gdprmerge", tableNameRoot, sqlMerge);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlMerge);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(stmt, denormUpdateTableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseGdprTest gdprtest.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	

}


