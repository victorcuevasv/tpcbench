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
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseWritePartitionedTest3 extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseWritePartitionedTest3(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseWritePartitionedTest3 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseWritePartitionedTest3 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseWritePartitionedTest3(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		//Override the default createTableReader to read from tables
		JarCreateTableReaderAsZipFile createTableReader = new JarCreateTableReaderAsZipFile(
						this.jarFile, "tables");
		List<String> unorderedList = createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQuery = createTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			writePartitioned(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void writePartitioned(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String tableName = tableNameRoot + "_partitioned";
			this.dropTable("drop table if exists " + tableName);
			sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
			String sqlCreate = SQLWritePartitionedTest3.createTableStatementSpark(sqlQuery, 
					tableNameRoot, tableName, this.format, this.extTablePrefixCreated, this.partition);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(sqlQuery, tableNameRoot, tableName);
			saveCreateTableFile("writepartitionedcreate", tableName, sqlCreate);
			String sqlInsert = SQLWritePartitionedTest3.insertStatement(sqlQuery, tableNameRoot, 
					tableName, this.format);
			saveCreateTableFile("writepartitionedinsert", tableName, sqlInsert);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlCreate);
			stmt.execute(sqlInsert);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseWritePartitionedTest3 writePartitioned.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStatementSnowflake(String sqlQuery, String tableNameRoot, 
			String tableName) {
		sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
		sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
		String clusterByKey = this.clusterByKeys.get(tableNameRoot);
		if( clusterByKey != null )
			sqlQuery = sqlQuery + "\n CLUSTER BY(" + clusterByKey + ")";
		return sqlQuery;
	}
	
	
}


