package org.bsc.dcc.vcv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class CreateSchemaSparkCLI extends CreateSchemaSpark {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;
	//private final String system;
	//private final String dbName;
	private final CommandLine commandLine;

	public CreateSchemaSparkCLI(String[] args) throws Exception {
		super();
		try {
			RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
			Options options = runOptions.getOptions();
			CommandLineParser parser = new DefaultParser();
			this.commandLine = parser.parse(options, args);
			String system = this.commandLine.getOptionValue("system-name");
			if( system.equals("sparkdatabricks") ) {
				this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						.getOrCreate();
			}
			else {
				this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						.enableHiveSupport()
						.getOrCreate();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSparkCLI constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			throw e;
		}
		this.system = this.commandLine.getOptionValue("system-name");
		this.dbName = this.commandLine.getOptionValue("schema-name");
	}


	public static void main(String[] args) {
		CreateSchemaSparkCLI application = null;
		try {
			application = new CreateSchemaSparkCLI(args);
		}
		catch(Exception e) {
			System.exit(1);
		}
		application.createSchema();
	}

	/*
	private void createSchema() {
		try {
			System.out.println("Creating schema (database) " + this.dbName + " with " + this.system);
			this.logger.info("Creating schema (database) " + this.dbName + " with " + this.system);
			this.spark.sql("CREATE DATABASE " + this.dbName);
			System.out.println("Schema (database) created.");
			this.logger.info("Schema (database) created.");
			//if( ! this.system.equals("sparkdatabricks") ) {
			//	this.closeConnection();
			//}
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
			this.logger.error(e);
		}
	}
	*/

}


