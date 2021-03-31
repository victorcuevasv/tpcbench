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
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkSparkOptions;
import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseSparkWriteUnPartitionedTest2 extends CreateDatabaseSparkDenormETLTask {
	
	
	public CreateDatabaseSparkWriteUnPartitionedTest2(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkWriteUnPartitionedTest2 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkWriteUnPartitionedTest2 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkWriteUnPartitionedTest2(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		this.useDatabase(this.dbName);
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
			if( ! this.format.equalsIgnoreCase("hudi") )
				writeUnPartitioned(fileName, sqlQuery, i);
			else
				writeUnPartitionedHudi(fileName, sqlQuery, i);
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
			String sqlCreate = SQLWriteUnPartitionedTest2.createTableStatementSpark(sqlQuery, 
					tableNameRoot, tableName, this.format, this.extTablePrefixCreated, false);
			saveCreateTableFile("writeunpartitionedcreate", tableName, sqlCreate);
			List<String> columns = CreateDatabaseSparkUtil.extractColumnNames(sqlQuery);
			String sqlInsert = SQLWriteUnPartitionedTest2.insertStatement(tableNameRoot, tableName,
					columns, "", this.format, false);
			saveCreateTableFile("writeunpartitionedinsert", tableName, sqlInsert);
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
			this.logger.error("Error in CreateDatabaseSparkWriteUnPartitionedTest2 writeUnPartitioned.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}

	
	private void writeUnPartitionedHudi(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String tableName = tableNameRoot + "_not_partitioned";
			String primaryKey = this.primaryKeys.get(tableNameRoot);
			String precombineKey = this.precombineKeys.get(tableNameRoot);
			Map<String, String> hudiOptions = null;
			hudiOptions = this.hudiUtil.createHudiOptions(tableName, 
						primaryKey, precombineKey, null, false);
			saveHudiOptions("hudi" + "writeunpartitioned", tableName, hudiOptions);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Dataset<Row> hudiDS = this.spark.read()
					.format("org.apache.hudi")
					.option("hoodie.datasource.query.type", "snapshot")
					.load(this.extTablePrefixCreated.get() + "/" + tableNameRoot + "/*");
			hudiDS.write().format("org.apache.hudi")
			  .option("hoodie.datasource.write.operation", "insert")
			  .options(hudiOptions).mode(SaveMode.Overwrite)
			  .save(this.extTablePrefixCreated.get() + "/" + tableName + "/");
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount ) {
				if( this.hudiUseMergeOnRead )
					this.countRowsQuery(tableName + "_ro");
				else
					this.countRowsQuery(tableName);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkWriteUnPartitionedTest2 writeUnPartitioned.");
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


