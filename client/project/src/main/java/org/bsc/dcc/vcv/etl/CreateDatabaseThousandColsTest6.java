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


public class CreateDatabaseThousandColsTest6 extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseThousandColsTest6(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseThousandColsTest6 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseThousandColsTest6 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseThousandColsTest6(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		//Override the default createTableReader to read from QueriesETLTest3
		JarCreateTableReaderAsZipFile createTableReader = new JarCreateTableReaderAsZipFile(
				this.jarFile, "QueriesETLTest6");
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
			merge(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void merge(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot + "_denorm_column_concat";
			System.out.println("Processing table " + index + ": " + tableNameRoot + "_denorm");
			this.logger.info("Processing table " + index + ": " + tableNameRoot + "_denorm");
			this.dropTable("drop table if exists " + tableName);
			String sqlCreate = SQLThousandColsTest6.createTableStatement(sqlQuery, 
					tableNameRoot, tableName, this.format, this.extTablePrefixCreated, this.partition);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(sqlQuery, tableName);
			saveCreateTableFile("denormthousandcols", tableName, sqlCreate);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlCreate);
			queryRecord.setSuccessful(true);
			if( this.doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseThousandColsTest6 merge.");
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
	
	
	private String createTableStatementSnowflake(String sqlQuery, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + tableName + " ");
		builder.append(sqlQuery);
		return builder.toString();
	}
	

}


