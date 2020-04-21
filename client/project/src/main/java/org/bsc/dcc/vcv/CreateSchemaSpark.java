package org.bsc.dcc.vcv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class CreateSchemaSpark {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String system;
	private final String dbName;
	private SparkSession spark;
	
	public CreateSchemaSpark(CommandLine commandLine) {
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						.enableHiveSupport()
						.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		this.system = commandLine.getOptionValue("system-name");
		this.dbName = commandLine.getOptionValue("schema-name");
	}
	
	/**
	 * @param args
	 * 
	 * args[0] system used to create the schema on the metastore
	 * args[1] schema (database) name
	 */
	public CreateSchemaSpark(String[] args) {
		if( args.length != 2 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		this.system = args[0];
		this.dbName = args[1];
		try {
			this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						.getOrCreate();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	public static void main(String[] args) {
		CreateSchemaSpark application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new CreateSchemaSpark(args);
		else {
			CommandLine commandLine = null;
			try {
				RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
				Options options = runOptions.getOptions();
				CommandLineParser parser = new DefaultParser();
				commandLine = parser.parse(options, args);
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error("Error in CreateSchemaSpark main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new CreateSchemaSpark(commandLine);
		}
		application.createSchema();
		//if( ! application.system.equals("sparkdatabricks") ) {
		//	application.closeConnection();
		//}
	}

	private void createSchema() {
		try {
			System.out.println("Creating schema (database) " + this.dbName + " with " + this.system);
			this.logger.info("Creating schema (database) " + this.dbName + " with " + this.system);
			this.spark.sql("CREATE DATABASE " + this.dbName);
			System.out.println("Schema (database) created.");
			this.logger.info("Schema (database) created.");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	private void closeConnection() {
		try {
			this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

}


