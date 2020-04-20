package org.bsc.dcc.vcv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class CreateSchemaSpark {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String system;
	private final String dbName;
	private SparkSession spark;

	public CreateSchemaSpark() {
		
	}
	
	public CreateSchemaSpark(String[] args) {
		this.system = args[0];
		this.dbName = args[1];
		try {
			if( this.system.equals("sparkdatabricks") ) {
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
		CreateSchemaSpark prog = new CreateSchemaSpark(args);
		prog.createSchema();
		//if( ! args[0].equals("sparkdatabricks") ) {
		//	prog.closeConnection();
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


