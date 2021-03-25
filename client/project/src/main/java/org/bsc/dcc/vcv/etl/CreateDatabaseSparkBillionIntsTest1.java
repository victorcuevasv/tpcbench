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
import org.apache.spark.sql.SparkSession;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.HudiUtil;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkSparkOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseSparkBillionIntsTest1 extends CreateDatabaseSparkDenormETLTask {

	
	public CreateDatabaseSparkBillionIntsTest1(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkBillionIntsTest1 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkBillionIntsTest1 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkBillionIntsTest1(commandLine);
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
			if( ! this.format.equalsIgnoreCase("hudi") )
				billionInts(fileName, sqlQuery, i);
			else
				billionIntsHudi(fileName, sqlQuery, i);
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
			String sqlCreate = SQLBillionIntsTest1.createTableStatement(sqlQuery, 
					tableName, this.format, this.extTablePrefixCreated);
			saveCreateTableFile("billionintscreate", tableName, sqlCreate);
			String sqlInsert = SQLBillionIntsTest1.insertStatement(sqlQuery, 
					tableNameRoot, tableName);
			saveCreateTableFile("billionintsinsert", tableName, sqlInsert);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			this.spark.sql(sqlCreate);
			this.spark.sql(sqlInsert);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(tableName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkBillionIntsTest1 billionInts.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private void billionIntsHudi(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot.charAt(0) + "s_item_sk";
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			String primaryKey = tableName;
			String precombineKey = tableName;
			Map<String, String> hudiOptions = null;
			hudiOptions = this.hudiUtil.createHudiOptions(primaryKey, 
						primaryKey, precombineKey, null, false);
			this.saveHudiOptions("hudi" + "billionints", tableNameRoot, hudiOptions);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			String selectSql = "SELECT " + primaryKey + " FROM " + tableNameRoot + "_temp";
			Dataset<Row> hudiDS = this.spark.read()
					.format("org.apache.hudi")
					.option("hoodie.datasource.query.type", "snapshot")
					.load(this.extTablePrefixCreated.get() + "/" + tableNameRoot + "/*");
			hudiDS.createOrReplaceTempView(tableNameRoot + "_temp");
			this.spark.sql(selectSql).write().format("org.apache.hudi")
			  .option("hoodie.datasource.write.operation", "insert")
			  .options(hudiOptions).mode(SaveMode.Overwrite)
			  .save(this.extTablePrefixCreated.get() + "/" + primaryKey + "/");
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					this.countRowsQuery(primaryKey + "_ro");
				else
					this.countRowsQuery(primaryKey);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkBillionIntsTest1 billionIntsHudi.");
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


