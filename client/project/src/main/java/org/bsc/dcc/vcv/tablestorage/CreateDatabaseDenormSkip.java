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


public class CreateDatabaseDenormSkip extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseDenormSkip(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseDenormSkip application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseDenormSkip main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseDenormSkip(commandLine);
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
			denormskip(fileName, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void denormskip(String sqlCreateFilename, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot + "_denorm_skip";
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			this.dropTable("drop table if exists " + tableName);
			String sqlCreate = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				sqlCreate = this.createTableStatementDatabricks(tableNameRoot, this.format,
					this.extTablePrefixCreated);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(tableNameRoot);
			saveCreateTableFile("denormskip", tableNameRoot, sqlCreate);
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
			this.logger.error("Error in CreateDatabaseDenormSkip denormskip.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStatementDatabricks(String tableNameRoot, String format,
			Optional<String> extTablePrefixCreated) {
		String skipAtt = this.skipKeys.get(tableNameRoot);
		String partCol = null;
		int posPart = Arrays.asList(Partitioning.tables).indexOf(tableNameRoot);
		if( posPart != -1 )
			partCol = Partitioning.partKeys[posPart];
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableNameRoot + "_denorm_skip \n");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy') \n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableNameRoot + 
				"_denorm_skip" + " '\n");
		if( this.partition ) {
				builder.append("PARTITIONED BY (" + partCol + ") \n" );
		}
		builder.append("AS \n");
		builder.append("SELECT * FROM " + tableNameRoot + "_denorm \n");
		builder.append(
				"WHERE MOD(" + partCol + ", " + SkipMods.firstMod + ") <> 0 \n");
		if( this.dateskThreshold != -1 )
			builder.append("OR " + partCol + " <= " + this.dateskThreshold + "\n");
		builder.append(
				"OR MOD(" + skipAtt + ", " + SkipMods.secondMod + ") <> 0 \n");
		if( this.partition && this.partitionWithDistrubuteBy )
				builder.append("DISTRIBUTE BY " + partCol + " \n" );
		return builder.toString();
	}
	
	
	private String createTableStatementSnowflake(String tableNameRoot) {
		String skipAtt = this.skipKeys.get(tableNameRoot);
		String partCol = null;
		int posPart = Arrays.asList(Partitioning.tables).indexOf(tableNameRoot);
		if( posPart != -1 )
			partCol = Partitioning.partKeys[posPart];
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableNameRoot + "_denorm_skip \n");
		builder.append("AS \n");
		builder.append("SELECT * FROM " + tableNameRoot + "_denorm \n");
		builder.append(
				"WHERE MOD(" + partCol + ", " + SkipMods.firstMod + ") <> 0 \n");
		if( this.dateskThreshold != -1 )
			builder.append("OR " + partCol + " <= " + this.dateskThreshold + "\n");
		builder.append(
				"OR MOD(" + skipAtt + ", " + SkipMods.secondMod + ") <> 0 \n");
		return builder.toString();
	}
	

}


