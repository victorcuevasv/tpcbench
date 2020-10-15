package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;

public class CreateSchema {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	//private static final String databricksDriverName = "com.simba.spark.jdbc42.Driver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private static final String synapseDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String redshiftDriverName = "com.amazon.redshift.jdbc42.Driver";
	
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final String hostname;
	private final String system;
	private final String dbName;
	private final String clusterId;
	
	public CreateSchema(CommandLine commandLine) {
		this.hostname = commandLine.getOptionValue("server-hostname");
		this.system = commandLine.getOptionValue("system-name");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.openConnection();
	}
	
	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] hostname of the server
	 * args[1] system used to create the schema on the metastore
	 * args[2] schema (database) name
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CreateSchema(String args[]) {
		if( args.length != 3 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		this.hostname = args[0];
		this.system = args[1];
		this.dbName = args[2];
		this.clusterId = "UNUSED";
		this.openConnection();
	}
	
	private void openConnection() {
		try {
			if( this.system.equals("hive") ) {
				Class.forName(driverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" + this.hostname + 
					":10000/", "hive", "");
			}
			else if( this.system.equals("presto") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8080/hive/", "hive", "");
			}
			else if( this.system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/", "hadoop", "");
			}
			else if( this.system.equals("sparkdatabricksjdbc") ) {
				String dbrToken = AWSUtil.getValue("DatabricksToken");
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://" + this.hostname + ":443/" +
				this.dbName + ";transportMode=http;ssl=1" + 
				";httpPath=sql/protocolv1/o/538214631695239/" + 
				this.clusterId + ";AuthMech=3;UID=token;PWD=" + dbrToken +
				";UseNativeQuery=1");
			}
			else if( this.system.equals("databrickssql") ) {
				// IMPORTANT: HAD TO HARDCODE THIS DUE TO LACK OF PERMISSION TO MANAGE SECRETS.
				// UPDATE TO PROPER PERMISSIONS WHEN TESTS ARE DONE.
				String dbrToken = "dapifd4db58404ae64629dc7b41d57f3a769";
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://"
					+ this.hostname + ":443/" + this.dbName
					+ ";transportMode=http;ssl=1;AuthMech=3"
					+ ";httpPath=/sql/1.0/endpoints/a57e3bc75ae9786b"
					+ ";UID=token;PWD=" + dbrToken
					+ ";UseNativeQuery=1");
			}
			else if( this.system.equals("redshift") ) {
				Class.forName(redshiftDriverName);
				this.con = DriverManager.getConnection("jdbc:redshift://" + this.hostname + ":5439/" +
				"dev" + "?ssl=true&UID=bsc-dcc-fjjm&PWD=Databr|cks1");
			}
			else if( this.system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/", "hive", "");
			}
			else if( this.system.startsWith("snowflake") ) {
				String snowflakePwd = AWSUtil.getValue("SnowflakePassword");
				Class.forName(snowflakeDriverName);
				this.con = DriverManager.getConnection("jdbc:snowflake://" + 
				"zua56993.snowflakecomputing.com" + "/?" +
						"user=bsctest&password=" + snowflakePwd);
			}
			else if( this.system.startsWith("synapse") ) {
				String synapsePwd = AWSUtil.getValue("SynapsePassword");
				Class.forName(synapseDriverName);
				this.con = DriverManager.getConnection("jdbc:sqlserver://" +
				this.hostname + ":1433;" +
				"database=bsc-tpcds-test-pool;" +
				"user=azureuser@bsctest;" +
				"password=" + synapsePwd + ";" +
				"encrypt=true;" +
				"trustServerCertificate=false;" +
				"hostNameInCertificate=*.database.windows.net;" +
				"loginTimeout=30;");
			}
			else if( this.system.startsWith("bigquery") ) {
				;
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + this.system);
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

	public static void main(String[] args) {
		CreateSchema application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new CreateSchema(args);
		else {
			CommandLine commandLine = null;
			try {
				RunBenchmarkOptions runOptions = new RunBenchmarkOptions();
				Options options = runOptions.getOptions();
				CommandLineParser parser = new DefaultParser();
				commandLine = parser.parse(options, args);
			}
			catch(Exception e) {
				e.printStackTrace();
				logger.error("Error in CreateSchema main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new CreateSchema(commandLine);
		}
		application.createSchema();
	}

	private void createSchema() {
		try {
			System.out.println("Creating schema (database) " + this.dbName + " with " + this.system);
			this.logger.info("Creating schema (database) " + this.dbName + " with " + this.system);
			if( system.startsWith("bigquery") ) {
				this.createDataset(this.dbName);
				return;
			}
			Statement stmt = this.con.createStatement();
			if( system.startsWith("presto") ) {
				stmt.execute("CREATE SCHEMA " + this.dbName);
			}
			else if( system.startsWith("spark") || system.startsWith("databricks") ) {
				stmt.execute("CREATE DATABASE " + this.dbName);
			}
			else if( system.startsWith("snowflake") ) {
				stmt.execute("CREATE DATABASE " + this.dbName);
				stmt.execute("USE DATABASE " + this.dbName);
				stmt.execute("CREATE SCHEMA " + this.dbName);
			}
			else if( system.startsWith("redshift") ) {
				stmt.execute("CREATE DATABASE " + this.dbName);
			}
			else if( system.startsWith("synapse") ) {
				stmt.execute("CREATE SCHEMA " + this.dbName);
				stmt.execute("ALTER USER tpcds_user WITH DEFAULT_SCHEMA = " + this.dbName);
				stmt.execute("GRANT ALTER ON SCHEMA::" + this.dbName + " TO tpcds_user");
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + this.system);
			}
			System.out.println("Schema (database) " + this.dbName + " created for " + this.system + ".");
			this.logger.info("Schema (database) " + this.dbName + " created for " + this.system + ".");
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}

		// Close the connection if using redshift as the driver leaves threads on the background that prevent the
		// application from closing. 
		if (this.system.equals("redshift") || this.system.equals("synapse"))
			this.closeConnection();
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
	
	private void createDataset(String datasetName) {
		try {
			// Initialize client that will be used to send requests.
			// This client only needs to be created once, and can be reused for multiple requests.
		    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

		    Dataset newDataset = bigquery.create(datasetInfo);
		    String newDatasetName = newDataset.getDatasetId().getDataset();
		    System.out.println(newDatasetName + " created successfully");
		}
		catch (BigQueryException e) {
			System.out.println("Dataset was not created. \n" + e.toString());
		}
	}

}


