package org.bsc.dcc.vcv.tablestorage;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
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


public class UpdateDatabaseInsUpdTest extends CreateDatabaseDenormETLTask {
	
	
	public UpdateDatabaseInsUpdTest(CommandLine commandLine) {
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		UpdateDatabaseInsUpdTest application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in UpdateDatabaseInsUpdTest main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new UpdateDatabaseInsUpdTest(commandLine);
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
			insupdtest(fileName, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void insupdtest(String sqlCreateFilename, int index) {
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
			String insertTableName = tableNameRoot + "_denorm" + "_insert_ten";
			String sqlMerge = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				sqlMerge = this.createMergeSQL(tableNameRoot,
						denormUpdateTableName, insertTableName);
			if( this.system.startsWith("snowflake") )
				sqlMerge = this.createMergeSQLSnowflake(tableNameRoot,
						denormUpdateTableName, insertTableName);
			saveCreateTableFile("insupdmerge", insertTableName, sqlMerge);
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
			this.logger.error("Error in UpdateDatabaseInsUpdTest insupdtest.");
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
			String insUpdTableName) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String primaryKeyFull = this.primaryKeys.get(tableNameRoot);
		StringTokenizer tokenizer = new StringTokenizer(primaryKeyFull, ",");
		String primaryKey = tokenizer.nextToken().trim();
		String primaryKeyComp = null;
		if( tokenizer.hasMoreTokens() )
			primaryKeyComp = tokenizer.nextToken().trim();
		StringBuilder builder = new StringBuilder();
		builder.append("MERGE INTO " + denormUpdateTableName + " AS a \n");
		builder.append("USING " + insUpdTableName + " AS b \n");
		builder.append("ON a." + partKey + " = b." + partKey + "\n");
		builder.append("AND a." + primaryKey + " = b." + primaryKey + "\n");
		if( primaryKeyComp != null )
			builder.append("AND a." + primaryKeyComp + " = b." + primaryKeyComp + "\n");
		builder.append("WHEN MATCHED THEN UPDATE SET * \n");
		builder.append("WHEN NOT MATCHED THEN INSERT * \n");
		return builder.toString();
	}
	
	
	private String createMergeSQLSnowflake(String tableNameRoot, String denormUpdateTableName, 
			String insUpdTableName) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String primaryKeyFull = this.primaryKeys.get(tableNameRoot);
		StringTokenizer tokenizer = new StringTokenizer(primaryKeyFull, ",");
		String primaryKey = tokenizer.nextToken().trim();
		String primaryKeyComp = null;
		if( tokenizer.hasMoreTokens() )
			primaryKeyComp = tokenizer.nextToken().trim();
		StringBuilder builder = new StringBuilder();
		builder.append("MERGE INTO " + denormUpdateTableName + " AS a \n");
		builder.append("USING " + insUpdTableName + " AS b \n");
		builder.append("ON a." + partKey + " = b." + partKey + "\n");
		builder.append("AND a." + primaryKey + " = b." + primaryKey + "\n");
		if( primaryKeyComp != null )
			builder.append("AND a." + primaryKeyComp + " = b." + primaryKeyComp + "\n");
		List<String> colNamesList = this.getColumnNamesList(denormUpdateTableName);
		String primaryKeyCompF = primaryKeyComp != null ? primaryKeyComp : "";
		List<String> colNamesListFiltered = colNamesList
				.stream()
				.filter(s -> ! s.contains(partKey))
				.filter(s -> ! s.contains(primaryKey))
				.filter(s -> ! s.contains(primaryKeyCompF))
				.collect(Collectors.toList());
		String matchedExpListStr = createMergeSQLSnowflakeMatch(colNamesListFiltered, "a", "b"); 
		builder.append("WHEN MATCHED THEN UPDATE SET " + matchedExpListStr + "\n");
		String noMatchedExpListStr = createMergeSQLSnowflakeNoMatch(colNamesList, "a", "b");
		builder.append("WHEN NOT MATCHED THEN INSERT " + noMatchedExpListStr + "\n");
		return builder.toString();
	}
	
	
	public String createMergeSQLSnowflakeMatch(List<String> colNamesList, String aliasA, 
			String aliasB) {
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < colNamesList.size() - 1; i++) {
			String col = colNamesList.get(i);
			builder.append(aliasA + "." + col + " = " + aliasB + "." + col + ",\n");
		}
		builder.append(aliasA + "." + colNamesList.get(colNamesList.size() - 1) + " = " + 
					aliasB + "." + colNamesList.get(colNamesList.size() - 1));
		return builder.toString();
	}
	
	
	public String createMergeSQLSnowflakeNoMatch(List<String> colNamesList, String aliasAUnused, 
			String aliasB) {
		StringBuilder namesBuilder = new StringBuilder("(");
		StringBuilder valsBuilder = new StringBuilder("(");
		for(int i = 0; i < colNamesList.size() - 1; i++) {
			String col = colNamesList.get(i);
			namesBuilder.append(col + ", ");
			valsBuilder.append(aliasB + "." + col + ",\n");
		}
		namesBuilder.append(colNamesList.get(colNamesList.size() - 1) + ")");
		valsBuilder.append(aliasB + "." + colNamesList.get(colNamesList.size() - 1) + ")");
		return namesBuilder.toString() + " VALUES " + valsBuilder.toString();
	}
	
	
	private List<String> getColumnNamesList(String denormTableName) {
		List<String> list = null;
		try {
			Statement stmt = this.con.createStatement();
			String describeQuery = "DESCRIBE " + denormTableName;
			if( this.system.contains("snowflake") )
				describeQuery = "DESCRIBE TABLE " + denormTableName;
			ResultSet rs = stmt.executeQuery(describeQuery);
			list = new ArrayList<String>();
			while ( rs.next() )
				list.add(rs.getString(1).toLowerCase());
			//Remove last three tuples since they do not represent columns in DBR SQL Analytics.
			if( this.system.contains("spark") || this.system.contains("databricks") ) {
				list.remove(list.size() - 1);
				list.remove(list.size() - 1);
				list.remove(list.size() - 1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("\"Error in CreateDatabaseInsertData getColumnNames.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return list;
	}
	

}


