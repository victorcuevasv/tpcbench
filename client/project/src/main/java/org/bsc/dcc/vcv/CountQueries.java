package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import com.google.cloud.bigquery.BigQueryException;

public class CountQueries {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private static final String synapseDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String redshiftDriverName = "com.amazon.redshift.jdbc42.Driver";
	
	private Connection con;
	private BigQueryDAO bigQueryDAO;
	private final JarCreateTableReaderAsZipFile createTableReader;
	private final AnalyticsRecorder recorder;
	
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	
	private final String test;
	private final int instance;
	private final String rawDataDir;
	private final String createTableDir;
	private final String suffix;
	
	private final Optional<String> extTablePrefixRaw;
	private final Optional<String> extTablePrefixCreated;
	private final String format;
	private final boolean doCount;
	private final boolean partition;
	private final int numCores;
	private final boolean orderedClustering;
	
	private final boolean bucketing;
	private final String hostname;
	private final String username;
	private final String jarFile;
	private final String dbPassword;
	
	//When data partitioning or bucketing is used, Presto has to be replaced by Hive.
	//This variable keeps track of that case.
	private String systemRunning;
	private final String createSingleOrAll;
	private final String clusterId;
	private final String userId;
	private final String columnDelimiter;
	private final Map<String, String> distKeys;
	private final Map<String, String> sortKeys;
	private final Map<String, String> clusterByKeys;
	
	public CountQueries(CommandLine commandLine) {
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "load");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.rawDataDir = commandLine.getOptionValue("raw-data-dir", "UNUSED");
		this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.suffix = commandLine.getOptionValue("text-file-suffix", "_ext");
		this.extTablePrefixRaw = Optional.ofNullable(commandLine.getOptionValue("ext-raw-data-location"));
		this.extTablePrefixCreated = Optional.ofNullable(commandLine.getOptionValue("ext-tables-location"));
		this.format = commandLine.getOptionValue("table-format");
		String doCountStr = commandLine.getOptionValue("count-queries", "true");
		this.doCount = Boolean.parseBoolean(doCountStr);
		String partitionStr = commandLine.getOptionValue("use-partitioning");
		this.partition = Boolean.parseBoolean(partitionStr);
		String bucketingStr = commandLine.getOptionValue("use-bucketing");
		this.bucketing = Boolean.parseBoolean(bucketingStr);
		this.hostname = commandLine.getOptionValue("server-hostname");
		this.username = commandLine.getOptionValue("connection-username");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.columnDelimiter = commandLine.getOptionValue("raw-column-delimiter", "SOH");
		this.userId = commandLine.getOptionValue("connection-username", "UNUSED");
		this.dbPassword = commandLine.getOptionValue("db-password", "UNUSED");
		String numCoresStr = commandLine.getOptionValue("num-cores", "-1");
		this.numCores = Integer.parseInt(numCoresStr);
		String orderedClusteringStr = commandLine.getOptionValue("ordered-clustering", "false");
		this.orderedClustering = Boolean.parseBoolean(orderedClusteringStr);
		this.distKeys = new DistKeys().getMap();
		this.sortKeys = new SortKeys().getMap();
		this.clusterByKeys = new ClusterByKeys().getMap();
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.systemRunning = this.system;
		if( commandLine.hasOption("override-load-system") ) {
			this.systemRunning = commandLine.getOptionValue("override-load-system");
		}
		this.openConnection();
	}
	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * 
	 * args[5] test name (i.e. load)
	 * args[6] experiment instance number
	 * args[7] directory for generated data raw files
	 * args[8] subdirectory within the jar that contains the create table files
	 * args[9] suffix used for intermediate table text files
	 * 
	 * args[10] prefix of external location for raw data tables (e.g. S3 bucket), null for none
	 * args[11] prefix of external location for created tables (e.g. S3 bucket), null for none
	 * args[12] format for column-storage tables (PARQUET, DELTA)
	 * args[13] whether to run queries to count the tuples generated (true/false)
	 * args[14] whether to use data partitioning for the tables (true/false)
	 * 
	 * args[15] whether to use bucketing for Hive and Presto
	 * args[16] hostname of the server
	 * args[17] username for the connection
	 * args[18] jar file
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public CountQueries(String[] args) {
		if( args.length != 19 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		this.workDir = args[0];
		this.dbName = args[1];
		this.resultsDir = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.rawDataDir = args[7];
		this.createTableDir = args[8];
		this.suffix = args[9];
		this.extTablePrefixRaw = Optional.ofNullable(
				args[10].equalsIgnoreCase("null") ? null : args[10]);
		this.extTablePrefixCreated = Optional.ofNullable(
				args[11].equalsIgnoreCase("null") ? null : args[11]);
		this.format = args[12];
		this.doCount = Boolean.parseBoolean(args[13]);
		this.partition = Boolean.parseBoolean(args[14]);
		this.bucketing = Boolean.parseBoolean(args[15]);
		this.hostname = args[16];
		this.username = args[17];
		this.createSingleOrAll = "all";
		this.clusterId = args[26];
		this.columnDelimiter = "SOH";
		this.userId = "UNUSED";
		this.dbPassword = "UNUSED";
		this.numCores = -1;
		this.orderedClustering = false;
		this.distKeys = new DistKeys().getMap();
		this.sortKeys = new SortKeys().getMap();
		this.clusterByKeys = new ClusterByKeys().getMap();
		this.jarFile = args[18];
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.systemRunning = this.system;
		if( this.system.equals("hive") || 
				( ( this.partition || this.bucketing ) && this.system.startsWith("presto") ) ) {
			this.systemRunning = "hive";
		}
		this.openConnection();
	}
	
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
						this.hostname + ":8080/hive/" + this.dbName, this.username, "");
			}
			else if( this.systemRunning.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				//Should use hadoop to drop a table created by spark.
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hadoop", "");
			}
			else if( this.systemRunning.equals("sparkdatabricksjdbc") ) {
				String dbrToken = AWSUtil.getValue("DatabricksToken");
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://" + this.hostname + ":443/" +
				this.dbName + ";transportMode=http;ssl=1" + 
				";httpPath=sql/protocolv1/o/538214631695239/" + 
				this.clusterId + ";AuthMech=3;UID=token;PWD=" + dbrToken +
				";UseNativeQuery=1");
			}
			else if( this.system.equals("databrickssql") ) {
				Class.forName(databricksDriverName);
				System.out.println("jdbc:spark://"
						+ this.hostname + ":443/" + this.dbName
						+ ";transportMode=http;ssl=1;AuthMech=3"
						+ ";httpPath=/sql/1.0/endpoints/" + this.clusterId
						+ ";UID=token;PWD=" + this.dbPassword
						+ ";UseNativeQuery=1"
						//+ ";spark.databricks.delta.optimizeWrite.binSize=" + this.numCores
						//+ ";spark.databricks.execution.resultCaching.enabled=false"
						//+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
						+ ";spark.databricks.delta.optimizeWrite.binSize=2048"
						+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
						//+ ";spark.driver.maxResultSize=0"
						+ ";spark.sql.shuffle.partitions=" + (this.numCores*2)
						//+ ";spark.databricks.delta.optimizeWrite.numShuffleBlocks=50000000"
						+ ";spark.databricks.delta.optimizeWrite.enabled=true"
					);
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
					+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
					//+ ";spark.driver.maxResultSize=0"
					+ ";spark.sql.shuffle.partitions=" + (this.numCores*2)
					//+ ";spark.databricks.delta.optimizeWrite.numShuffleBlocks=50000000"
					+ ";spark.databricks.delta.optimizeWrite.enabled=true"
				);
			}
			else if( this.system.equals("redshift") ) {
				Class.forName(redshiftDriverName);
				this.con = DriverManager.getConnection("jdbc:redshift://" + this.hostname + ":5439/" +
				this.dbName + "?ssl=true&UID=" + this.userId + "&PWD=" + this.dbPassword);
			}
			else if( this.systemRunning.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			else if( this.systemRunning.startsWith("snowflake") ) {
				String snowflakePwd = AWSUtil.getValue("SnowflakePassword");
				Class.forName(snowflakeDriverName);
				this.con = DriverManager.getConnection("jdbc:snowflake://" + 
						this.hostname + "/?" +
						"user=" + this.userId + "&password=" + snowflakePwd +
						"&warehouse=" + this.clusterId + "&schema=" + this.dbName);
			}
            else if( this.system.startsWith("synapse") ) {
                String synapsePwd = this.dbPassword; //AWSUtil.getValue("SynapsePassword");
                Class.forName(synapseDriverName);
                this.con = DriverManager.getConnection("jdbc:sqlserver://" +
                this.hostname + ":1433;" +
                "database=" + this.clusterId + ";" +
                "user=tpcds_user@cdw-2021;" +
                "password=" + synapsePwd + ";" +
                "encrypt=true;" +
                "trustServerCertificate=false;" +
                "hostNameInCertificate=*.sql.azuresynapse.net;" +
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
	
	public static void main(String[] args) throws SQLException {
		CountQueries application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new CountQueries(args);
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
				logger.error("Error in CreateDatabase main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new CountQueries(commandLine);
		}
		application.executeCountQueries();
	}
	
	private void executeCountQueries() {
		if( this.system.startsWith("snowflake") ) {
			this.useDatabaseQuery(this.dbName);
			this.useSchemaQuery(this.dbName);
			this.useSnowflakeWarehouseQuery(this.clusterId);
		}
		this.executeQuery("show tables");
		// Process each .sql create table file found in the jar file.
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlCreate = this.createTableReader.getFile(fileName);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if( fileName.equals("dbgen_version.sql") ) {
				System.out.println("Skipping: " + fileName);
				continue;
			}
			if( ! this.createSingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.createSingleOrAll) ) {
					System.out.println("Skipping: " + fileName);
					continue;
				}
			}
			String tableName = fileName.substring(0, fileName.indexOf('.'));
			this.countRowsQuery(tableName);
			i++;
		}
	}	
	
	private void useDatabaseQuery(String dbName) {
		try {
			Statement stmt = con.createStatement();
			stmt.executeUpdate("USE DATABASE " + dbName);
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
		
	private void countRowsQuery(String tableName) {
		try {
			Statement stmt = con.createStatement();
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
	
	private int executeQuery(String sql)  {
		int tuples = 0;
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData metadata = rs.getMetaData();
			int nCols = metadata.getColumnCount();
			tuples = 0;
			while (rs.next()) {
				StringBuilder rowBuilder = new StringBuilder();
				for (int i = 1; i <= nCols - 1; i++) {
					rowBuilder.append(rs.getString(i) + " | ");
				}
				rowBuilder.append(rs.getString(nCols));
				System.out.println(rowBuilder.toString());
				tuples++;
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
		return tuples;
	}
	
	public void closeConnection() {
		try {
			this.con.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

}


