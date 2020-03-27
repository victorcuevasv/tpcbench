package org.bsc.dcc.vcv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class CreateSchemaSpark {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private SparkSession spark;

	public CreateSchemaSpark(String system) {
		try {
			if( system.equals("sparkdatabricks") ) {
				this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						//	.enableHiveSupport()
						.getOrCreate();
				//this.logger.info(SparkUtil.stringifySparkConfiguration(this.spark));
			}
			else {
				this.spark = SparkSession.builder().appName("TPC-DS Database Creation")
						.enableHiveSupport()
						.getOrCreate();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	/**
	 * @param args
	 * 
	 * args[0] system used to create the schema on the metastore
	 * args[1] schema (database) name
	 */
	public static void main(String[] args) {
		if( args.length != 2 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		CreateSchemaSpark prog = new CreateSchemaSpark(args[0]);
		prog.createSchema(args[0], args[1]);
		if( ! args[0].equals("sparkdatabricks") ) {
			prog.closeConnection();
		}
	}

	private void createSchema(String system, String dbName) {
		try {
			System.out.println("Creating schema (database) " + dbName + " with " + system);
			this.logger.info("Creating schema (database) " + dbName + " with " + system);
			this.spark.sql("CREATE DATABASE " + dbName);
			System.out.println("Schema (database) created.");
			this.logger.info("Schema (database) created.");
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

}


