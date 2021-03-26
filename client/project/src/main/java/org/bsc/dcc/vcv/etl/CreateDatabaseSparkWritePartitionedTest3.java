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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.RunBenchmarkSparkOptions;


public class CreateDatabaseSparkWritePartitionedTest3 extends CreateDatabaseSparkDenormETLTask {
	
	
	public CreateDatabaseSparkWritePartitionedTest3(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkWritePartitionedTest3 application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkWritePartitionedTest3 main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkWritePartitionedTest3(commandLine);
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
				writePartitioned(fileName, sqlQuery, i);
			else
				writePartitionedHudi(fileName, sqlQuery, i);
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
			String tableName = tableNameRoot + "_partitioned";
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			this.dropTable("drop table if exists " + tableName);
			sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
			String sqlCreate = SQLWritePartitionedTest3.createTableStatementSpark(sqlQuery, tableNameRoot,
					tableName, this.format, this.extTablePrefixCreated, true);
			saveCreateTableFile("writepartitionedcreate", tableName, sqlCreate);
			String sqlInsert = SQLWritePartitionedTest3.insertStatement(sqlQuery, tableNameRoot, 
					tableName, this.format);
			saveCreateTableFile("writepartitionedinsert", tableName, sqlInsert);
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
			this.logger.error("Error in CreateDatabaseSparkWritePartitionedTest3 writePartitioned.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private void writePartitionedHudi(String sqlCreateFilename, String sqlQuery, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String tableName = tableNameRoot + "_partitioned";
			String primaryKey = this.primaryKeys.get(tableNameRoot);
			String precombineKey = this.precombineKeys.get(tableNameRoot);
			Map<String, String> hudiOptions = null;
			String partitionKey = 
						Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
			hudiOptions = this.hudiUtil.createHudiOptions(tableName, 
						primaryKey, precombineKey, partitionKey, true);
			saveHudiOptions("hudi" + "writepartitioned", tableName, hudiOptions);
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
			this.logger.error("Error in CreateDatabaseSparkWritePartitionedTest3 writePartitioned.");
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


