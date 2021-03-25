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
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseDenormTest4 extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseDenormTest4(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseDenormTest4 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseDenormTest4 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseDenormTest4(commandLine);
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
			denorm(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void denorm(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot + "_denorm";
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			this.dropTable("drop table if exists " + tableName);
			String sqlCreate = this.createTableStatement(sqlQuery, tableNameRoot, this.format,
					this.extTablePrefixCreated);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(sqlQuery, tableNameRoot);
			saveCreateTableFile("denorm", tableName, sqlCreate);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlCreate);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseDenormTest4 denorm.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStatement(String sqlQuery, String tableName, String format,
			Optional<String> extTablePrefixCreated) {
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableName + "_denorm\n");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "_denorm" + "' \n");
		if( this.partition ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
			if( pos != -1 )
				builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n" );
		}
		builder.append("AS\n");
		builder.append(sqlQuery + "\n");
		if( this.denormWithFilter && this.filterKeys.get(tableName) != null ) {
			builder.append("AND " + this.filterKeys.get(tableName) + " = " + 
					this.filterValues.get(tableName) + "\n");
		}
		if( this.partition && this.partitionWithDistrubuteBy ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
			if( pos != -1 )
				builder.append("DISTRIBUTE BY " + Partitioning.partKeys[pos] + " \n" );
		}
		return builder.toString();
	}
	
	
	private String createTableStatementSnowflake(String sqlQuery, String tableName) {
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableName + "_denorm\n");
		builder.append("AS\n");
		builder.append(sqlQuery + "\n");
		if( this.denormWithFilter && this.filterKeys.get(tableName) != null ) {
			builder.append("AND " + this.filterKeys.get(tableName) + " = " + 
					this.filterValues.get(tableName) + "\n");
		}
		return builder.toString();
	}
	

}


