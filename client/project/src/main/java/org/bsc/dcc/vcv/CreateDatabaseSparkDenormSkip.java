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
import org.bsc.dcc.vcv.etl.CreateDatabaseSparkDenormETLTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseSparkDenormSkip extends CreateDatabaseSparkDenormETLTask {
	
	
	public CreateDatabaseSparkDenorm(CommandLine commandLine) {	
		super(commandLine);
	}
	

	public static void main(String[] args) throws SQLException {
		CreateDatabaseSparkDenormSkip application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseSparkDenormSkip main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseSparkDenormSkip(commandLine);
		application.createTables();
	}
	
	
	private void createTables() {
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

	
	private void useDatabase(String dbName) {
		try {
			this.spark.sql("USE " + dbName);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenormSkip useDatabase.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void createTable(String sqlCreateFilename, String sqlQueryIgnored, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			this.dropTable("drop table if exists " + tableName + "_denorm_skip");
			String skipAtt = this.skipKeys.get(tableName);
			String partCol = null;
			int posPart = Arrays.asList(Partitioning.tables).indexOf(tableName);
			if( posPart != -1 )
				partCol = Partitioning.partKeys[posPart];
			String sqlSelect = "SELECT * FROM " + tableName + "_denorm \n";
			StringBuilder selectBuilder = new StringBuilder(sqlSelect);
			selectBuilder.append(
					"WHERE MOD(" + partCol + ", " + SkipMods.firstMod + ") <> 0 \n");
			if( this.dateskThreshold != -1 )
				selectBuilder.append("OR " + partCol + " <= " + this.dateskThreshold + "\n");
			selectBuilder.append(
					"OR MOD(" + skipAtt + ", " + SkipMods.secondMod + ") <> 0");
			if( this.partition && this.partitionWithDistrubuteBy ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					selectBuilder.append("DISTRIBUTE BY " + Partitioning.partKeys[pos] + " \n" );
			}
			sqlSelect = selectBuilder.toString();
			if( this.doCount )
				countRowsQuery(tableName + "_denorm");
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			if( ! this.partition ) {
				this.spark.sql(sqlSelect).write()
				.option("compression", "snappy")
				.option("path", extTablePrefixCreated.get() + "/" + tableName + "_denorm_skip")
				.mode("overwrite")
				.format("parquet")
				.saveAsTable(tableName + "_denorm_skip");
			}
			else {
				this.spark.sql(sqlSelect).write()
				.option("compression", "snappy")
				.option("path", extTablePrefixCreated.get() + "/" + tableName + "_denorm_skip")
				.partitionBy(partCol)
				.mode("overwrite")
				.format("parquet")
				.saveAsTable(tableName + "_denorm_skip");
			}
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			saveCreateTableFile("parquetdenormskip", tableName, sqlSelect);
			if( this.doCount )
				countRowsQuery(tableName + "_denorm_skip");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenormSkip createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private void dropTable(String dropStmt) {
		try {
			this.spark.sql(dropStmt);
		}
		catch(Exception ignored) {
			//Do nothing.
		}
	}

	
	public void saveCreateTableFile(String suffix, String tableName, String sqlCreate) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".sql";
			File temp = new File(createTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(createTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(sqlCreate);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}

	
	private void countRowsQuery(String tableName) {
		try {
			String sqlCount = "select count(*) from " + tableName;
			System.out.print("Running count query on " + tableName + ": ");
			this.logger.info("Running count query on " + tableName + ": ");
			Dataset<Row> countDataset = this.spark.sql(sqlCount);
			List<String> list = countDataset.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
			for(String s: list) {
				System.out.println(s);
				this.logger.info("Count result: " + s);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	
	public void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkDenormSkip closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


