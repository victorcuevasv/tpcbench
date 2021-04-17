package org.bsc.dcc.vcv.tablestorage;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.etl.CreateDatabaseDenormETLTask;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.bsc.dcc.vcv.SkipMods;
import org.bsc.dcc.vcv.UpdateMods;
import org.bsc.dcc.vcv.SkipKeys;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseDeleteData extends CreateDatabaseDenormETLTask {
	
	
	private final double[] fractions = {0.1, 1.0, 10.0};
	private final String[] deleteSuffix = {"pointone", "one", "ten"};
	private final int[] firstMod = {10, 10, 3};
	private final int[] secondMod = {100, 10, 3};
	private final int[] firstEqual = {1, 0, 0};
	private final int[] secondEqual = {0, 0, 0};
	
	
	public CreateDatabaseDeleteData(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseDeleteData application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseDeleteData main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseDeleteData(commandLine);
		application.doTask();
	}
	
	
	protected void doTask() {
		// Process each .sql create table file found in the jar file.
		if( this.system.contains("spark") || this.system.contains("databricks") )
			this.useDatabaseQuery(this.dbName);
		else if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		this.recorder.header();
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlQueryUnused = this.createTableReader.getFile(fileName);
			if( ! this.denormSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.denormSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			for(int j = 0; j < this.fractions.length; j++)
				deletedata(fileName, i, j);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void deletedata(String sqlCreateFilename, int index, int fractionIndex) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String denormTableName = tableNameRoot + "_denorm";
			String deleteTableName = denormTableName + "_delete_" + this.deleteSuffix[fractionIndex];
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			this.dropTable("drop table if exists " + deleteTableName);
			String sqlCreate = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				sqlCreate = this.createTableStmtDatabricks(tableNameRoot, 
						denormTableName, deleteTableName, this.format,
					this.extTablePrefixCreated, fractionIndex);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStmtSnowflake(tableNameRoot, 
						denormTableName, deleteTableName, fractionIndex);
			saveCreateTableFile("deletecreate", tableNameRoot, sqlCreate);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlCreate);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(stmt, deleteTableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseDeleteData deletedata.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStmtDatabricks(String tableNameRoot, String denormTableName, 
			String deleteTableName, String format, Optional<String> extTablePrefixCreated,
			int fractionIndex) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String skipAtt = this.skipKeys.get(tableNameRoot);
		StringBuilder builder = new StringBuilder("CREATE TABLE " + deleteTableName + "\n");
		builder.append("USING " + format + "\n");
		if( format.equalsIgnoreCase("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + deleteTableName + "' \n");
		builder.append("AS\n");
		builder.append("SELECT * FROM " + denormTableName + "\n");
		builder.append("WHERE MOD(" + partKey + ", " + 
				this.firstMod[fractionIndex] + ") = " + this.firstEqual[fractionIndex] + "\n");
		if( this.dateskThreshold != -1 )
			builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
		builder.append("AND MOD(" + skipAtt + ", " + 
				this.secondMod[fractionIndex] + ") = " + this.secondEqual[fractionIndex] + "\n");
		return builder.toString();
	}
	
	
	private String createTableStmtSnowflake(String tableNameRoot, String denormTableName, 
			String deleteTableName, int fractionIndex) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String skipAtt = this.skipKeys.get(tableNameRoot);
		StringBuilder builder = new StringBuilder("CREATE TABLE " + deleteTableName + "\n");
		builder.append("AS\n");
		builder.append("SELECT * FROM " + denormTableName + "\n");
		builder.append("WHERE MOD(" + partKey + ", " + 
				this.firstMod[fractionIndex] + ") = " + this.firstEqual[fractionIndex] + "\n");
		if( this.dateskThreshold != -1 )
			builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
		builder.append("AND MOD(" + skipAtt + ", " + 
				this.secondMod[fractionIndex] + ") = " + this.secondEqual[fractionIndex] + "\n");
		return builder.toString();
	}
	

}


