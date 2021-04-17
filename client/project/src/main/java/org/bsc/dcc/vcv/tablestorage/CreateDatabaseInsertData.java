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


public class CreateDatabaseInsertData extends CreateDatabaseDenormETLTask {
	
	
	public CreateDatabaseInsertData(CommandLine commandLine) {	
		super(commandLine);
	}
	
	
	public static void main(String[] args) throws SQLException {
		CreateDatabaseInsertData application = null;
		CommandLine commandLine = null;
		try {
			RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			commandLine = parser.parse(options, args);
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.error("Error in CreateDatabaseInsertData main.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		application = new CreateDatabaseInsertData(commandLine);
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
			insertdata(fileName, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
	}
	
	
	private void insertdata(String sqlCreateFilename, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableNameRoot = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			String tableName = tableNameRoot + "_denorm_skip";
			System.out.println("Processing table " + index + ": " + tableNameRoot);
			this.logger.info("Processing table " + index + ": " + tableNameRoot);
			String denormTableName = tableName + "_denorm";
			String insertTableName = denormTableName + "_insert_ten";
			this.dropTable("drop table if exists " + insertTableName);
			String sqlCreate = null;
			if( this.system.contains("spark") || this.system.contains("databricks") )
				sqlCreate = this.createTableStatementDatabricks(tableNameRoot, 
						denormTableName, insertTableName, this.format,
					this.extTablePrefixCreated);
			if( this.system.startsWith("snowflake") )
				sqlCreate = this.createTableStatementSnowflake(tableNameRoot, 
						denormTableName, insertTableName);
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
			this.logger.error("Error in CreateDatabaseInsertData insertdata.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				this.recorder.record(queryRecord);
			}
		}
	}
	
	
	private String createTableStatementDatabricks(String tableNameRoot, String denormTableName, 
			String insertTableName, String format, Optional<String> extTablePrefixCreated) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String skipAtt = this.skipKeys.get(tableNameRoot);
		StringBuilder builder = new StringBuilder("CREATE TABLE " + insertTableName + "\n");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + insertTableName + "' \n");
		builder.append("AS \n");
		builder.append("( SELECT * FROM " + denormTableName + "\n");
		builder.append("WHERE MOD(" + partKey + ", " + SkipMods.firstMod + ") = 0 \n");
		if( this.dateskThreshold != -1 )
			builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
		builder.append("AND MOD(" + skipAtt + ", " + SkipMods.secondMod + ") = 0 ) \n");
		String updateExpr = this.createUpdatesExpression(denormTableName, partKey, skipAtt);
		builder.append("UNION ALL\n");
		builder.append(updateExpr);
		return builder.toString();
	}
	
	
	private String createUpdatesExpression(String denormTableName, String partKey, String skipAtt) {
		String expr = null;
		try {
			String columnsStr = getColumnNames(denormTableName);
			String columnsStrUpd = columnsStr.replace("s_quantity", "s_quantity + 1");
			StringBuilder builder = new StringBuilder();
			builder.append(
					"( SELECT \n" +
					 columnsStrUpd + "\n" +
					 "FROM " + denormTableName + "\n" +
					 "WHERE MOD(" + partKey + ", " + UpdateMods.firstMod + ") = 1 \n"
					 );
					 if( this.dateskThreshold != -1 )
							builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
			builder.append("AND MOD(" + skipAtt + ", " + UpdateMods.secondMod + ") = 0 ) \n");
			expr = builder.toString();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseInsertData createUpdatesExpression.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return expr;
	}
	
	
	private String createTableStatementSnowflake(String tableNameRoot, String denormTableName, 
			String insertTableName) {
		String partKey = 
				Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableNameRoot)];
		String skipAtt = this.skipKeys.get(tableNameRoot);
		StringBuilder builder = new StringBuilder("CREATE TABLE " + insertTableName + "\n");
		builder.append("AS \n");
		builder.append("( SELECT * FROM " + denormTableName + "\n");
		builder.append("WHERE MOD(" + partKey + ", " + SkipMods.firstMod + ") = 0 \n");
		if( this.dateskThreshold != -1 )
			builder.append("AND " + partKey + " > " + this.dateskThreshold + "\n");
		builder.append("AND MOD(" + skipAtt + ", " + SkipMods.secondMod + ") = 0 ) \n");
		String updateExpr = this.createUpdatesExpression(denormTableName, partKey, skipAtt);
		builder.append("UNION ALL\n");
		builder.append(updateExpr);
		return builder.toString();
	}
	
	
	private String getColumnNames(String denormTableName) {
		String retVal = null;
		try {
			Statement stmt = this.con.createStatement();
			ResultSet rs = stmt.executeQuery("DESCRIBE " + denormTableName);
			ResultSetMetaData metadata = rs.getMetaData();
			int nCols = metadata.getColumnCount();
			List<String> list = new ArrayList<String>();
			for (int i = 1; i <= nCols - 1; i++) {
				String colName = metadata.getColumnName(i);
				list.add(colName);
			}
			String columnsStr = list.stream()
					.collect(Collectors.joining(", \n"));
			retVal = columnsStr;
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("\"Error in CreateDatabaseInsertData getColumnNames.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		return retVal;
	}
	

}


