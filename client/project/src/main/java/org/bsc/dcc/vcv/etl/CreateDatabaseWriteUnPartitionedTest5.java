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


public class CreateDatabaseWriteUnPartitionedTest5 extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseWriteUnPartitionedTest5(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseWriteUnPartitionedTest5 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseWriteUnPartitionedTest5 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseWriteUnPartitionedTest5(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		this.useDatabaseQuery(this.dbName);
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
			writeUnPartitioned(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void writeUnPartitioned(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String tableName = tableNameRoot + "_not_partitioned";
			this.dropTable("drop table if exists " + tableName);
			sqlQuery = this.incompleteCreateTable(sqlQuery);
			sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
			StringBuilder builder = new StringBuilder(sqlQuery + "\n");
			builder.append("USING " + format.toUpperCase() + "\n");
			if( this.format.equals("parquet") )
				builder.append("OPTIONS ('compression'='snappy')\n");
			builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
			String sqlCreate = builder.toString();
			saveCreateTableFile("writeunpartitionedcreate", tableName, sqlCreate);
			String sqlInsert = "insert into " + tableName + " select * from " + tableNameRoot;
			saveCreateTableFile("writeunpartitionedinsert", tableName, sqlInsert);
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
			this.logger.error("Error in CreateDatabaseWriteUnPartitionedTest5 writeUnPartitioned.");
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
	
	
	private String incompleteCreateTable(String sqlCreate) {
		boolean hasPrimaryKey = sqlCreate.contains("primary key");
		// Remove the 'not null' statements.
		sqlCreate = sqlCreate.replace("not null", "        ");
		// Split the SQL create table statement in lines.
		String lines[] = sqlCreate.split("\\r?\\n");
		// Add all of the lines in the original SQL to the new statement, except those
		// which contain the primary key statement (if any) and the closing parenthesis.
		// For the last column statement, remove the final comma.
		StringBuilder builder = new StringBuilder();
		int tail = hasPrimaryKey ? 3 : 2;
		for (int i = 0; i < lines.length - tail; i++)
			builder.append(lines[i] + "\n");
		// Change the last comma for a space (since the primary key statement was
		// removed).
		char[] commaLineArray = lines[lines.length - tail].toCharArray();
		commaLineArray[commaLineArray.length - 1] = ' ';
		builder.append(new String(commaLineArray) + "\n");
		// Close the parenthesis.
		builder.append(") \n");
		// Version 2.1 of Hive does not recognize integer, so use int instead.
		return builder.toString().replace("integer", "int    ");
	}

	
}


