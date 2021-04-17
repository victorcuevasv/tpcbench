package org.bsc.dcc.vcv.tablestorage;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringTokenizer;
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


public class UpdateDatabaseDeleteTest extends CreateDatabaseDenormETLTask {
	
	
	private final double[] fractions = {0.1, 1.0, 10.0};
	private final String[] deleteSuffix = {"pointone", "one", "ten"};
	
	
	public UpdateDatabaseDeleteTest(CommandLine commandLine) {
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		UpdateDatabaseDeleteTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseDeleteTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseDeleteTest(commandLine);
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
				deletetest(fileName, i, j);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void deletetest(String sqlCreateFilename, int index, int fractionIndex) {
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
			String denormUpdateTableName = tableNameRoot + "_denorm_" + suffix;
			String deleteTableName = tableNameRoot + "_denorm_" + "_delete_" + 
					this.deleteSuffix[fractionIndex];
			String sqlMerge = null;
			if( this.system.contains("spark") || this.system.contains("databricks")
					|| this.system.startsWith("snowflake") )
				sqlMerge = this.createMergeSQL(tableNameRoot,
						denormUpdateTableName, deleteTableName);
			saveCreateTableFile("deletemerge", deleteTableName, sqlMerge);
			Statement stmt = this.con.createStatement();
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(sqlMerge);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			if( this.doCount )
				countRowsQuery(stmt, denormUpdateTableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in UpdateDatabaseDeleteTest deletetest.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createMergeSQL(String tableNameRoot, String denormUpdateTableName, 
			String deleteTableName) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String primaryKeyFull = this.primaryKeys.get(tableNameRoot);
		StringTokenizer tokenizer = new StringTokenizer(primaryKeyFull, ",");
		String primaryKey = tokenizer.nextToken();
		String primaryKeyComp = null;
		if( tokenizer.hasMoreTokens() )
			primaryKeyComp = tokenizer.nextToken();
		StringBuilder builder = new StringBuilder();
		builder.append("MERGE INTO " + denormUpdateTableName + " AS a \n");
		builder.append("USING " + deleteTableName + " AS b \n");
		builder.append("ON a." + partKey + " = b." + partKey + "\n");
		builder.append("AND a." + primaryKey + " = b." + primaryKey + "\n");
		if( primaryKeyComp != null )
			builder.append("AND a." + primaryKeyComp + " = b." + primaryKeyComp + "\n");
		builder.append("WHEN MATCHED THEN DELETE \n");
		return builder.toString();
	}
	

}


