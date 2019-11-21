package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class CreateSchema {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc41.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CreateSchema(String hostname, String system) {
		try {
			if( system.equals("hive") ) {
				Class.forName(driverName);
				con = DriverManager.getConnection("jdbc:hive2://" + hostname + 
					":10000/", "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8889/hive/", "hadoop", "");
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/", "hive", "");
			}
			else if( system.equals("sparkdatabricksjdbc") ) {
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://" + hostname + ":443/default" +
				";transportMode=http;ssl=1" + 
				";httpPath=sql/protocolv1/o/538214631695239/" + 
				"<cluster name>;AuthMech=3;UID=token;PWD=<personal-access-token>" +
				";UseNativeQuery=1");
			}
			else if( system.startsWith("snowflake") ) {
				Class.forName(snowflakeDriverName);
				con = DriverManager.getConnection("jdbc:snowflake://zua56993.snowflakecomputing.com/?" +
						"user=bsctest&password=c4[*4XYM1GIw");
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + system);
			}
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] hostname of the server
	 * args[1] system used to create the schema on the metastore
	 * args[2] schema (database) name
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length != 3 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		CreateSchema prog = new CreateSchema(args[0], args[1]);
		prog.createSchema(args[2], args[1]);
		prog.closeConnection();
	}

	private void createSchema(String dbName, String system) {
		try {
			System.out.println("Creating schema (database) " + dbName + " with " + system);
			this.logger.info("Creating schema (database) " + dbName + " with " + system);
			Statement stmt = con.createStatement();
			if( system.startsWith("presto") )
				stmt.execute("CREATE SCHEMA " + dbName);
			else if( system.startsWith("spark") )
				stmt.execute("CREATE DATABASE " + dbName);
			else if( system.startsWith("snowflake") ) {
				stmt.execute("CREATE DATABASE " + dbName);
				stmt.execute("USE DATABASE " + dbName);
				stmt.execute("CREATE SCHEMA " + dbName);
			}
			System.out.println("Schema (database) created.");
			this.logger.info("Schema (database) created.");
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	public void closeConnection() {
		try {
			this.con.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

}


