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
import org.bsc.dcc.vcv.etl.CreateDatabaseSparkDenormETLTask;


public class CreateDatabaseSparkDenorm extends CreateDatabaseSparkDenormETLTask {
	
	
	public CreateDatabaseSparkDenorm(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkDenorm application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkDenorm main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkDenorm(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		this.useDatabase(this.dbName);
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
			createTable(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void createTable(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			this.dropTable("drop table if exists " + tableName + "_denorm");
			String sqlCreate = this.createTableStatement(sqlQuery, tableName, this.format,
					this.extTablePrefixCreated);
			saveCreateTableFile("denorm", tableName, sqlCreate);
			if( this.doCount )
				countRowsQuery(tableName);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(sqlCreate);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(tableName + "_denorm");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenorm createTable.");
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
	

}


