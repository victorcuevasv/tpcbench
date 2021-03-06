package org.bsc.dcc.vcv.tablestorage;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Optional;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.Partitioning;
import org.bsc.dcc.vcv.QueryRecord;
import org.bsc.dcc.vcv.etl.CreateDatabaseDenormETLTask;
import org.bsc.dcc.vcv.RunBenchmarkOptions;
import org.bsc.dcc.vcv.SkipMods;
import org.bsc.dcc.vcv.SkipKeys;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CreateDatabaseDenormUpdate extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseDenormUpdate(CommandLine commandLine) {
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseDenormUpdate application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseDenormUpdate main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseDenormUpdate(commandLine);
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
			loadupdate(fileName, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void loadupdate(String sqlCreateFilename, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String suffix = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				suffix = "delta";
			else
				suffix = "update";
			String tableName = tableNameRoot + "_denorm_" + suffix;
			this.dropTable("drop table if exists " + tableName);
			String sqlCreate = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				sqlCreate = this.createTableStmtDatabricks(tableNameRoot,
					this.extTablePrefixCreated);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(tableNameRoot);
			saveCreateTableFile(suffix + "denorm", tableNameRoot, sqlCreate);
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
			this.logger.error("Error in CreateDatabaseDenormUpdate loadupdate.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStmtDatabricks(String tableNameRoot,
			Optional<String> extTablePrefixCreated) {
		String partCol = null;
		int posPart = Arrays.asList(Partitioning.tables).indexOf(tableNameRoot);
		if( posPart != -1 )
			partCol = Partitioning.partKeys[posPart];
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableNameRoot + "_denorm_delta \n");
		builder.append("USING DELTA \n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableNameRoot + 
				"_denorm_delta'" + "\n");
		if( this.partition ) {
				builder.append("PARTITIONED BY (" + partCol + ") \n" );
		}
		builder.append("AS \n");
		builder.append("SELECT * FROM " + tableNameRoot + "_denorm_skip \n");
		return builder.toString();
	}
	
	
	private String createTableStatementSnowflake(String tableNameRoot) {
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableNameRoot + "_denorm_update \n");
		builder.append("AS \n");
		builder.append("SELECT * FROM " + tableNameRoot + "_denorm_skip \n");
		return builder.toString();
	}
	

}


