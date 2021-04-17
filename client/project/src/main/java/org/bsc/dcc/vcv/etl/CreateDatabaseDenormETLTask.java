package org.bsc.dcc.vcv.etl;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Optional;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bsc.dcc.vcv.AWSUtil;
import org.bsc.dcc.vcv.AnalyticsRecorder;
import org.bsc.dcc.vcv.AppUtil;
import org.bsc.dcc.vcv.BigQueryDAO;
import org.bsc.dcc.vcv.ClusterByKeys;
import org.bsc.dcc.vcv.FilterKeys;
import org.bsc.dcc.vcv.FilterValues;
import org.bsc.dcc.vcv.HudiPrecombineKeys;
import org.bsc.dcc.vcv.HudiPrimaryKeys;
import org.bsc.dcc.vcv.JarCreateTableReaderAsZipFile;
import org.bsc.dcc.vcv.SkipKeys;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public abstract class CreateDatabaseDenormETLTask {

	protected static final Logger logger = LogManager.getLogger("AllLog");
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private static final String synapseDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String redshiftDriverName = "com.amazon.redshift.jdbc42.Driver";
	protected Connection con;
	protected BigQueryDAO bigQueryDAO;
	protected String systemRunning;
	protected final JarCreateTableReaderAsZipFile createTableReader;
	protected final AnalyticsRecorder recorder;
	protected final String workDir;
	protected final String dbName;
	protected final String resultsDir;
	protected final String experimentName;
	protected final String system;
	protected final String test;
	protected final int instance;
	protected final String createTableDir;
	protected final Optional<String> extTablePrefixCreated;
	protected final String format;
	protected final boolean doCount;
	protected final boolean partition;
	protected final String jarFile;
	protected final String createSingleOrAll;
	protected final String denormSingleOrAll;
	protected final Map<String, String> precombineKeys;
	protected final Map<String, String> filterKeys;
	protected final Map<String, String> filterValues;
	protected final Map<String, String> skipKeys;
	protected final String clusterId;
	protected final String hostname;
	protected final String dbPassword;
	protected final int numCores;
	protected final String userId;
	protected final boolean partitionWithDistrubuteBy;
	protected final boolean denormWithFilter;
	protected final Map<String, String> clusterByKeys;
	private final boolean useCachedResultSnowflake = false;
	protected final int dateskThreshold;
	protected final Map<String, String> primaryKeys;
	protected final String customerSK;
	
	
	public CreateDatabaseDenormETLTask(CommandLine commandLine) {
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "loaddenorm");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		//this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.createTableDir = "QueriesDenorm";
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		this.format = commandLine.getOptionValue("table-format");
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.denormSingleOrAll = commandLine.getOptionValue("denorm-all-or-file", "all");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.precombineKeys = new HudiPrecombineKeys().getMap();
		this.filterKeys = new FilterKeys().getMap();
		this.filterValues = new FilterValues().getMap();
		this.skipKeys = new SkipKeys().getMap();
		this.systemRunning = this.system;
		if( commandLine.hasOption("override-load-system") ) {
			this.systemRunning = commandLine.getOptionValue("override-load-system");
		}
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.hostname = commandLine.getOptionValue("server-hostname");
		this.dbPassword = commandLine.getOptionValue("db-password", "UNUSED");
		String numCoresStr = commandLine.getOptionValue("num-cores", "-1");
		this.numCores = Integer.parseInt(numCoresStr);
		this.userId = commandLine.getOptionValue("connection-username", "UNUSED");
		String partitionWithDistrubuteByStr = commandLine.getOptionValue(
				"partition-with-distribute-by", "false");
		this.partitionWithDistrubuteBy = Boolean.parseBoolean(partitionWithDistrubuteByStr);
		String denormWithFilterStr = commandLine.getOptionValue(
				"denorm-with-filter", "false");
		this.denormWithFilter = Boolean.parseBoolean(denormWithFilterStr);
		this.clusterByKeys = new ClusterByKeys().getMap();
		String dateskThresholdStr = commandLine.getOptionValue("datesk-gt-threshold", "-1");
		this.dateskThreshold = Integer.parseInt(dateskThresholdStr);
		this.primaryKeys = new HudiPrimaryKeys().getMap();
		this.customerSK = commandLine.getOptionValue("gdpr-customer-sk");
		this.openConnection();
	}
	
	
	protected abstract void doTask();

	
	private void openConnection() {
		try {
			//IMPORTANT.
			//Use Hive instead of Presto due to out of memory errors when using partitioning.
			//The logs would still be organized as if Presto was used.
			if( this.systemRunning.equals("hive") ) {
				Class.forName(driverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" + this.hostname + 
					":10000/" + dbName, "hive", "");
			}
			else if( this.systemRunning.equals("presto") ) {
				Class.forName(prestoDriverName);
				//this.con = DriverManager.getConnection("jdbc:presto://" + 
				//		this.hostname + ":8080/hive/" + this.dbName, "hive", "");
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8080/hive/" + this.dbName, this.userId, "");
			}
			else if( this.systemRunning.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hadoop", "");
			}
			else if( this.systemRunning.equals("sparkdatabricksjdbc") ) {
				//String dbrToken = AWSUtil.getValue("DatabricksToken");
				String dbrToken = this.dbPassword;
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://" + this.hostname + ":443/" +
				this.dbName + ";transportMode=http;ssl=1" + 
				";httpPath=sql/protocolv1/o/538214631695239/" + 
				this.clusterId + ";AuthMech=3;UID=token;PWD=" + dbrToken +
				";UseNativeQuery=1");
			}
			else if( this.system.equals("databrickssql") ) {
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://"
					+ this.hostname + ":443/" + this.dbName
					+ ";transportMode=http;ssl=1;AuthMech=3"
					+ ";httpPath=/sql/1.0/endpoints/" + this.clusterId
					+ ";UID=token;PWD=" + this.dbPassword
					+ ";UseNativeQuery=1"
					//+ ";spark.databricks.delta.optimizeWrite.binSize=" + this.numCores
					//+ ";spark.databricks.execution.resultCaching.enabled=false"
					//+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
					+ ";spark.databricks.delta.optimizeWrite.binSize=2048"
					//+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
					//+ ";spark.driver.maxResultSize=0"
					+ ";spark.sql.shuffle.partitions=" + (this.numCores*2)
					//+ ";spark.databricks.delta.optimizeWrite.numShuffleBlocks=50000000"
					+ ";spark.databricks.delta.optimizeWrite.enabled=true"
				);
			}
			else if( this.system.equals("redshift") ) {
				Class.forName(redshiftDriverName);
				//Use Synapse's password temporarily (must be specified when creating the cluster)
				String redshiftPwd = AWSUtil.getValue("SynapsePassword");
				this.con = DriverManager.getConnection("jdbc:redshift://" + this.hostname + ":5439/" +
				this.dbName + "?ssl=true&UID=" + this.userId + "&PWD=" + redshiftPwd);
			}
			else if( this.systemRunning.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			else if( this.systemRunning.startsWith("snowflake") ) {
				//String snowflakePwd = AWSUtil.getValue("SnowflakePassword");
				String snowflakePwd = this.dbPassword;
				Class.forName(snowflakeDriverName);
				this.con = DriverManager.getConnection("jdbc:snowflake://" + 
						this.hostname + "/?" +
						"user=" + this.userId + "&password=" + snowflakePwd +
						"&warehouse=" + this.clusterId + "&schema=" + this.dbName);
			}
			else if( this.system.startsWith("synapse") ) {
				String synapsePwd = AWSUtil.getValue("SynapsePassword");
				Class.forName(synapseDriverName);
				this.con = DriverManager.getConnection("jdbc:sqlserver://" +
				this.hostname + ":1433;" +
				"database=bsc-tpcds-test-pool;" +
				"user=tpcds_user_loader@bsctest;" +
				"password=" + synapsePwd + ";" +
				"encrypt=true;" +
				"trustServerCertificate=false;" +
				"hostNameInCertificate=*.database.windows.net;" +
				"loginTimeout=30;");
			}
			else if( this.system.startsWith("bigquery") ) {
				this.bigQueryDAO = new BigQueryDAO("databricks-bsc-benchmark", this.dbName);
			}
			else {
				throw new java.lang.RuntimeException("Unsupported system: " + this.systemRunning);
			}
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
	}
	
	
	protected void useDatabaseQuery(String dbName) {
		try {
			Statement stmt = this.con.createStatement();
			String query = "USE DATABASE " + dbName;
			if( this.systemRunning.contains("spark") )
				query = "USE " + dbName;
			stmt.executeUpdate(query);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	
	private void useSchemaQuery(String schemaName) {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("USE SCHEMA " + schemaName);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	
	private void useSnowflakeWarehouseQuery(String warehouseName) {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("USE WAREHOUSE " + warehouseName);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	
	private void setSnowflakeDefaultSessionOpts() {
		try {
			Statement sessionStmt = this.con.createStatement();
			sessionStmt.executeUpdate("ALTER SESSION SET USE_CACHED_RESULT = " + this.useCachedResultSnowflake);
			sessionStmt.close();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in setSnowflakeDefaultSessionOpts");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	protected void prepareSnowflake() {
		this.useDatabaseQuery(this.dbName);
		this.useSchemaQuery(this.dbName);
		this.useSnowflakeWarehouseQuery(this.clusterId);	
	}
	
	
	protected void dropTable(String dropStmt) {
		try {
			Statement stmt = this.con.createStatement();
			stmt.executeUpdate(dropStmt);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

	
	public void saveCreateTableFile(String suffix, String tableName, String sqlCreate) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".sql";
			File temp = new File(createTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(createTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(sqlCreate);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}

	
	protected void countRowsQuery(Statement stmt, String tableName) {
		try {
			String sql = "select count(*) from " + tableName;
			System.out.print("Running count query on " + tableName + ": ");
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
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
			this.logger.error("Error in CreateDatabaseDenormETLTask closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

}


