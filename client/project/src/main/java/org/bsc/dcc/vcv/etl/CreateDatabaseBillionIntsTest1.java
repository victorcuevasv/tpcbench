package org.bsc.dcc.vcv.etl;

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
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseBillionIntsTest1 extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseBillionIntsTest1(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseBillionIntsTest1 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseBillionIntsTest1 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseBillionIntsTest1(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQuery = this.createTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			billionInts(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void billionInts(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot.charAt(0) + "s_item_sk";
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			this.dropTable("drop table if exists " + tableName);
			String sqlCreate = CreateDatabaseBillionIntsTest1.createTableStatement(sqlQuery, 
					tableName, this.format, this.extTablePrefixCreated);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(sqlQuery, tableName);
			saveCreateTableFile("billionintscreate", tableName, sqlCreate);
			String sqlInsert = CreateDatabaseBillionIntsTest1.insertStatement(sqlQuery, 
					tableNameRoot, tableName);
			saveCreateTableFile("billionintsinsert", tableName, sqlInsert);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlCreate);
			stmt.execute(sqlInsert);
			queryRecord.setSuccessful(true);
			if( this.doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseBillionIntsTest1 billionInts.");
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
	

	public static String createTableStatement(String sqlQuery, String tableName,
			String format, Optional<String> extTablePrefixCreated) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + tableName + " ");
		builder.append("(" + tableName + " int) ");
		builder.append("USING " + format);
		if( this.format.equals("parquet") )
			builder.append("\nOPTIONS ('compression'='snappy')");
		builder.append("\nLOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	
	private String createTableStatementSnowflake(String sqlQuery, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + tableName + " ");
		builder.append("(" + tableName + " int) ");
		return builder.toString();
	}
	
	
	public static String insertStatement(String sqlQuery, String tableNameRoot, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		builder.append("SELECT " + tableName + "\n");
		builder.append("FROM " + tableNameRoot + "\n");
		return builder.toString();
	}
	
	
}


