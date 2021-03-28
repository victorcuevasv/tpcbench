package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.sql.SQLException;
import org.apache.logging.log4j.Logger;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.bsc.dcc.vcv.etl.CreateDatabaseSparkDenormETLTask;


public class CreateDatabaseSparkDenormSkip extends CreateDatabaseSparkDenormETLTask {
	
	
	public CreateDatabaseSparkDenormSkip(CommandLine commandLine) {	
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
			createTable(fileName, sqlQuery, i);
			i++;
		}
		//if( ! this.system.equals("sparkdatabricks") ) {
		//	this.closeConnection();
		//}
		this.recorder.close();
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
					"OR MOD(" + skipAtt + ", " + SkipMods.secondMod + ") <> 0 \n");
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
	

}


