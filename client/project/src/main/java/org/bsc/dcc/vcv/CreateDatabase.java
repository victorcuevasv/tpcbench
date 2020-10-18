package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
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

public class CreateDatabase {

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
	
	public CreateDatabase(CommandLine commandLine) {
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
		String doCountStr = commandLine.getOptionValue("count-queries", "false");
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
	public CreateDatabase(String[] args) {
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
		this.clusterId = "UNUSED";
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
				+ ";UseNativeQuery=1");
				this.con = DriverManager.getConnection("jdbc:spark://"
					+ this.hostname + ":443/" + this.dbName
					+ ";transportMode=http;ssl=1;AuthMech=3"
					+ ";httpPath=/sql/1.0/endpoints/" + this.clusterId
					+ ";UID=token;PWD=" + this.dbPassword
					+ ";UseNativeQuery=1");
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
				String synapsePwd = AWSUtil.getValue("SynapsePassword");
				Class.forName(synapseDriverName);
				this.con = DriverManager.getConnection("jdbc:sqlserver://" +
				this.hostname + ":1433;" +
				"database=bsc-tpcds-test-pool;" +
				"user=tpcds_user@bsctest;" +
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
	
	public static void main(String[] args) throws SQLException {
		CreateDatabase application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new CreateDatabase(args);
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
			application = new CreateDatabase(commandLine);
		}
		application.createTables();
	}
	
	private void createTables() {
		if( this.system.startsWith("snowflake") ) {
			this.useDatabaseQuery(this.dbName);
			this.useSchemaQuery(this.dbName);
			this.useSnowflakeWarehouseQuery(this.clusterId);
			this.createSnowflakeStageQuery(this.dbName + "_stage");
		}
		if( this.system.startsWith("databrickssql") && (this.numCores > 0)) {
			try {
				Statement stmt = con.createStatement();
				// Set the number of shuffle partitions (default 200) to the number of cores to be able to load large datasets.
				stmt.execute("SET spark.sql.shuffle.partitions = " + this.numCores + ";");
				//stmt.execute("SET spark.databricks.delta.optimizeWrite.binSize = 2048;");
				//stmt.execute("SET spark.databricks.adaptive.autoOptimizeShuffle.enabled = true;");
				//stmt.execute("SET spark.driver.maxResultSize = 0;"); // DBR SQL does not allow to change this with the cluster running
				//stmt.execute("SET spark.databricks.delta.optimizeWrite.numShuffleBlocks = 50000000;");
				//stmt.execute("SET spark.databricks.delta.optimizeWrite.enabled = true;");
				
			}	catch (Exception e) {
					e.printStackTrace();
					this.logger.error("Error in CreateDatabaseSpark createTable.");
					this.logger.error(e);
					this.logger.error(AppUtil.stringifyStackTrace(e));
				}
		}
		// Process each .sql create table file found in the jar file.
		this.recorder.header();
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
			if (this.systemRunning.equals("snowflake")) {
				this.createTableSnowflake(fileName, sqlCreate, i);
			}
			else if (this.systemRunning.equals("redshift")) {
				this.createTableRedshift(fileName, sqlCreate, i);
			}
			else if (this.systemRunning.equals("synapse")) {
				this.createTableSynapse(fileName, sqlCreate, i);
			}
			else if (this.systemRunning.equals("databrickssql"))
				this.createTableDatabricksSQL(fileName, sqlCreate, i);
			else if (this.systemRunning.equals("bigquery"))
				this.createTableBigQuery(fileName, sqlCreate, i);
			else
				this.createTable(fileName, sqlCreate, i);
			i++;
		}
		this.recorder.close();
		//Close the connection if using redshift as the driver leaves threads on the background
		//that prevent the application from closing. 
		if (this.system.equals("redshift") || this.system.equals("synapse"))
			this.closeConnection();
	}
	
	private void createTableSnowflake(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		String suffix = "";
		try {
			//First, create the table, no format or options are specified, only the schema data.
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			//Hive and Spark use the statement 'create external table ...' for raw data tables
			String snowflakeSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, suffix, false);
			String clusterByKey = this.clusterByKeys.get(tableName);
			if( clusterByKey != null )
				snowflakeSqlCreate = snowflakeSqlCreate + "\n CLUSTER BY(" + clusterByKey + ")";
			saveCreateTableFile("snowflaketable", tableName, snowflakeSqlCreate);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + suffix);
			stmt.execute(snowflakeSqlCreate);
			//Upload the .dat files to the table stage, which is created by default.
			String putSql = null;
			if( ! this.rawDataDir.equals("UNUSED") ) {
				putSql = "PUT file://" + this.rawDataDir + "/" + tableName + "/*.dat @%" + tableName;
				saveCreateTableFile("snowflakeput", tableName, putSql);
				stmt.execute(putSql);
			}
			String fieldDelimiter = "'\\\\001'";
			if( this.columnDelimiter.equals("PIPE") )
				fieldDelimiter = "'|'";
			String copyIntoSql = null;
			//A null extTablePrefixRaw indicates to use local files for table creation.
			if( ! this.extTablePrefixRaw.isPresent() )
				copyIntoSql = "COPY INTO " + tableName + " FROM " + "'@%" + tableName + "' \n" +
								"FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\\\001' ENCODING = 'ISO88591'" +
								"\n ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE EMPTY_FIELD_AS_NULL = TRUE " +
								"\n DATE_FORMAT = 'YYYY-MM-DD')";
			//Otherwise, extTablePrefixRaw indicates the Snowflake stage associated with the S3 bucket.
			else 
				copyIntoSql = "COPY INTO " + tableName + " FROM " + 
						//"@" + this.extTablePrefixRaw.get() + "/" + tableName + "/ \n" +
						//Update: the name of the stage is formed by this.dbName + "_stage"
						"@" + this.dbName + "_stage" + "/" + tableName + "/ \n" +
						"FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = " + fieldDelimiter + " ENCODING = 'ISO88591'" +
						"\n ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE EMPTY_FIELD_AS_NULL = TRUE " +
						"\n DATE_FORMAT = 'YYYY-MM-DD')";
			saveCreateTableFile("snowflakecopy", tableName, copyIntoSql);
			stmt.execute(copyIntoSql);
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}

	private void createTableBigQuery(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		String suffix = "";
		try {
			//First, create the table, no format or options are specified, only the schema data.
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			//Hive and Spark use the statement 'create external table ...' for raw data tables
			String bigQuerySqlCreate = incompleteCreateTable(sqlCreate, tableName, false, suffix, false);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			bigQuerySqlCreate = this.bigQueryDAO.createTable(tableName, bigQuerySqlCreate);
			saveCreateTableFile("bigquerytable", tableName, bigQuerySqlCreate);
			//A null extTablePrefixRaw indicates to use local files for table creation.
			if( ! this.extTablePrefixRaw.isPresent() ) {
				//TODO
			}
			else {
				String sourceUri = this.extTablePrefixRaw.get() + "/" + tableName + "/*";
				this.bigQueryDAO.loadCsvFromGcs(tableName, sourceUri, this.columnDelimiter);
			}
			queryRecord.setSuccessful(true);
			if( doCount ) {
				this.bigQueryDAO.countQuery(tableName);
			}
		}
		catch (BigQueryException | InterruptedException) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}
	
	private void createTableSynapse(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		String suffix = "";
		try {
			//First, create the table, no format or options are specified, only the schema data.
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			//Hive and Spark use the statement 'create external table ...' for raw data tables
			String synapseSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, suffix, false);
			String distKey = this.distKeys.get(tableName);
			String sortKey = this.sortKeys.get(tableName);
			synapseSqlCreate += "WITH( DISTRIBUTION = ";
			if( distKey.equals("all") )
				synapseSqlCreate += "REPLICATE";
			else if( distKey.equals("none") )
				synapseSqlCreate += "ROUND ROBIN";
			else
				synapseSqlCreate += "HASH(" + distKey + ")";
			synapseSqlCreate += ", CLUSTERED COLUMNSTORE INDEX";
			if( ! sortKey.equals("none") )
				synapseSqlCreate += " ORDER(" + sortKey + ")";
			synapseSqlCreate += ")\n";
			saveCreateTableFile("synapsetable", tableName, synapseSqlCreate);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			stmt.execute("if object_id ('" + tableName + suffix +  "','U') is not null drop table " + 
					tableName + suffix);
			stmt.execute(synapseSqlCreate);
			String synapseToken = AWSUtil.getValue("SynapseToken");
			String fieldTerminator = "'0x01'";
			if( this.columnDelimiter.equals("PIPE") )
				fieldTerminator = "'|'";
			String copySql = "COPY INTO " + tableName + " FROM '" + 
						this.extTablePrefixRaw.get() + "/" + tableName + "/' \n" +
						"WITH (" + 
						"\tFILE_TYPE = 'CSV', \n" + 
						"\tFIELDTERMINATOR = " + fieldTerminator + ", \n" +
						"\tROWTERMINATOR = '0x0A', \n" + 
						"\tCREDENTIAL=(IDENTITY= 'Shared Access Signature', " +
						"SECRET='<SECRET>') \n" +
						") \n";
			saveCreateTableFile("synapsecopy", tableName, copySql);
			copySql = copySql.replace("<SECRET>", synapseToken);
			stmt.execute(copySql);
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTableSynapse.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}

	private void createTableRedshift(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		String suffix = "";

		try {
			//First, create the table, no format or options are specified, only the schema data.
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);

			// The DDL provided by TPC works out of the box on Redshift, we only have to add the distribution and partition 
			// keys without the last semicolon and newline character (The reader ast a trailing newline)
			String redshiftSqlCreate = sqlCreate.substring(0, sqlCreate.length()-2);
			// Get the distribution key for the table (usually the PK) and add the relevant statement. If the table is to be distributed to
			// all nodes (distkey "all") then add "diststyle all" instead.
			String distKey = this.distKeys.get(tableName);
			if (distKey.equals("all")) redshiftSqlCreate += " diststyle all";
			else if (distKey != null) redshiftSqlCreate += (" distkey(" + distKey + ")");

			// Add the sorkey if one has been assigned to the table
			String sortKey = this.sortKeys.get(tableName);
			if (!sortKey.equals("none")) redshiftSqlCreate += (" sortkey(" + sortKey + ")");
			redshiftSqlCreate += ";";
			saveCreateTableFile("redshifttable", tableName, redshiftSqlCreate);
			queryRecord = new QueryRecord(index);
			
			Statement stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + suffix);

			String fieldDelimiter = "'\001'";
			if( this.columnDelimiter.equals("PIPE") )
				fieldDelimiter = "'|'";
			String copySql = "copy " + tableName + " from " + 
					"'" + this.extTablePrefixRaw.get() + "/" + tableName + "/' \n" +
					"iam_role 'arn:aws:iam::384416317380:role/tpcds-redshift'\n" +
					"delimiter " + fieldDelimiter + "\n" +
					"ACCEPTINVCHARS\n" +
					"STATUPDATE OFF\n" +
					"EMPTYASNULL\n" +
					"region 'us-west-2';";

			saveCreateTableFile("redshiftcopy", tableName, copySql);	// Save the string to file after stopping recording time.

			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.execute(redshiftSqlCreate);
			
			// Move the data directly from S3 into Redshift through COPY. Invalid chars need to be accepted due to one of the tuples having an
			// special comma.
			
			stmt.execute(copySql);
			queryRecord.setSuccessful(true);
			
			if( doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}

	private void createTableDatabricksSQL(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		Statement stmt = null;
		String suffix = "_ext";

		try {
			// Create the external table as explained by Mostafa
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			StringBuilder extSb = new StringBuilder(incompleteCreateTable(sqlCreate, tableName, false, "_ext", false));
			String fieldDelimiter = "'\001'";
			if( this.columnDelimiter.equals("PIPE")) fieldDelimiter = "'|'";
			extSb.append("USING CSV\n");
			extSb.append("OPTIONS(\n");
			extSb.append("  path='" + this.extTablePrefixRaw.get() + "/" + tableName + "',\n");
			extSb.append("  inferSchchema='false',\n");
			extSb.append("  sep="+fieldDelimiter+",\n");
			extSb.append("  header='false',\n");
			extSb.append("  emptyValue='',\n");
			extSb.append("  nullValue='',\n");
			extSb.append("  charset='iso-8859-1',\n");
			extSb.append("  dateFormat='yyy-MM-dd',\n");
			extSb.append("  timestampFormat='yyyy-MM-dd HH:mm:ss[.SSS]', -- spec: yyyy-mm-dd hh:mm:ss.s\n");
			extSb.append("  mode='PERMISSIVE',\n");
			extSb.append("  multiLine='false',\n");
			extSb.append("  locale='en-US',\n");
			extSb.append("  lineSep='\\n'\n");
			extSb.append(");");
			
			String extSqlCreate = extSb.toString();
			
			saveCreateTableFile("csv", tableName, extSqlCreate);			

			System.out.println("Dropping table " + tableName + "_ext");
			// Drop the external table if it exists
			stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + this.suffix);

			// If count is enabled, count the number of rows and print them to console
			if( this.doCount ) {
				countRowsQuery(stmt, tableName + this.suffix);
			}
			
			// Generate the internal create table sql and write it to file
			String incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
			String intSqlCreate = internalCreateTableDatabricks(incIntSqlCreate, tableName, this.extTablePrefixCreated, this.format);
			saveCreateTableFile(format, tableName, intSqlCreate);
			// Drop the internal table if it exists
			stmt.execute("drop table if exists " + tableName);

			StringBuilder sbInsert = new StringBuilder("INSERT OVERWRITE TABLE ");
			sbInsert.append(tableName); sbInsert.append(" SELECT ");
			
			//	sbInsert.append(" /*+ COALESCE(" + this.numCores + ") */ ");
			sbInsert.append("* FROM "); sbInsert.append(tableName); sbInsert.append(suffix); sbInsert.append("\n");
			if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName))
			{
				String partKey = Partitioning.distKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
				String distKey = this.distKeys.get(tableName);
				sbInsert.append("DISTRIBUTE BY CASE WHEN " + partKey + " IS NOT NULL THEN " + partKey + " ELSE " + distKey + " % 601 END;\n");
			}
			String insertSql = sbInsert.toString();

			// Save the Insert Overwrite file
			saveCreateTableFile("insert", tableName, insertSql);
			stmt = con.createStatement();
			// Start measuring time just before running the actual load
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());

			System.out.println("Creating table " + tableName + "_ext");
			// Create again the external table
			stmt.execute(extSqlCreate);

			System.out.println("Creating table " + tableName);
			// Create the internal table
			stmt = con.createStatement();
			stmt.execute(intSqlCreate);

			// Insert into the delta table
			System.out.println("Inserting data into table " + tableName);

			stmt.execute(insertSql);
			
			// If enabled, count the number of rows
			queryRecord.setSuccessful(true);
			if( this.doCount ) {
				countRowsQuery(stmt, tableName + this.suffix);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabaseSpark createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}
	
	// To create each table from the .dat file, an external table is first created.
	// Then a parquet table is created and data is inserted into it from the
	// external table.
	// The SQL create table statement found in the file has to be modified for
	// creating these tables.
	private void createTable(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Processing table " + index + ": " + tableName);
			this.logger.info("Processing table " + index + ": " + tableName);
			//Hive and Spark use the statement 'create external table ...' for raw data tables
			String incExtSqlCreate = incompleteCreateTable(sqlCreate, tableName, 
					! this.systemRunning.startsWith("presto"), suffix, false);
			String extSqlCreate = null;
			if( this.systemRunning.equals("hive") || this.systemRunning.startsWith("spark"))
				extSqlCreate = externalCreateTableHive(incExtSqlCreate, tableName, rawDataDir, 
						extTablePrefixRaw);
			else if( this.systemRunning.startsWith("presto") )
				extSqlCreate = externalCreateTablePresto(incExtSqlCreate, tableName, rawDataDir,
						extTablePrefixRaw);
			saveCreateTableFile("textfile", tableName, extSqlCreate);
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			stmt.execute("drop table if exists " + tableName + suffix);
			stmt.execute(extSqlCreate);
			if( doCount )
				countRowsQuery(stmt, tableName + suffix);
			String incIntSqlCreate = null;
			String intSqlCreate = null;
			if( this.systemRunning.equals("hive") || this.systemRunning.startsWith("spark")) {
				//For Hive the partition attribute should NOT be included in the create table attributes list.
				incIntSqlCreate = null;
				if( this.partition )
					incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", true);
				else
					incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
				intSqlCreate = internalCreateTableHive(incIntSqlCreate, tableName, extTablePrefixCreated, format);
			}
			else if( this.systemRunning.startsWith("presto") ) {
				//For Presto the create table statement should include all of the attributes, but the partition
				//attribute should be the last.
				incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
				if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
					String partitionAtt = Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
					incIntSqlCreate = shiftPartitionColumn(incIntSqlCreate, partitionAtt);
				}
				intSqlCreate = internalCreateTablePresto(incIntSqlCreate, tableName, extTablePrefixCreated, format);
			}
			saveCreateTableFile(format, tableName, intSqlCreate);
			stmt.execute("drop table if exists " + tableName);
			stmt.execute(intSqlCreate);
			String insertSql = null;
			if( this.systemRunning.equals("hive") || this.systemRunning.startsWith("spark")) {
				insertSql = "INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tableName + suffix;
				if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
					//The partition attribute was removed from the attributes list in the create table
					//statement for Hive, so the columns should be extracted as it is the case for Presto.
					incIntSqlCreate = incompleteCreateTable(sqlCreate, tableName, false, "", false);
					String partitionAtt = Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)];
					incIntSqlCreate = shiftPartitionColumn(incIntSqlCreate, partitionAtt);
					List<String> columns = extractColumnNames(incIntSqlCreate);
					insertSql = createPartitionInsertStmt(tableName, columns, suffix, "INSERT OVERWRITE TABLE " + 
							tableName + " PARTITION (" + partitionAtt + ")");
				}
				else if( this.bucketing && Arrays.asList(Bucketing.tables).contains(tableName) ) {
					//The bucketing attribute is NOT removed from the attributes list in the create table
					//statement for Hive, the columns should be inserted normally.
					//So the insert statement does not need to be modified.
					;
				}
			}
			else if( this.systemRunning.startsWith("presto") ) {
				insertSql = "INSERT INTO " + tableName + " SELECT * FROM " + tableName + suffix;
				if( this.partition && Arrays.asList(Partitioning.tables).contains(tableName) ) {
					List<String> columns = extractColumnNames(incIntSqlCreate); 
					insertSql = createPartitionInsertStmt(tableName, columns, suffix, "INSERT INTO " + tableName);
				}
			}
			saveCreateTableFile("insert", tableName, insertSql);
			stmt.execute(insertSql);
			queryRecord.setSuccessful(true);
			if( doCount )
				countRowsQuery(stmt, tableName);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in CreateDatabase createTable.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			if( queryRecord != null ) {
				queryRecord.setEndTime(System.currentTimeMillis());
				this.recorder.record(queryRecord);
			}
		}
	}
	
	// Generate an incomplete SQL create statement to be completed for the texfile
	// external and
	// parquet internal tables.
	private String incompleteCreateTable(String sqlCreate, String tableName, boolean external, String suffix,
			boolean checkPartitionKey) {
		boolean hasPrimaryKey = sqlCreate.contains("primary key");
		// Remove the 'not null' statements.
		sqlCreate = sqlCreate.replace("not null", "        ");
		// Split the SQL create table statement in lines.
		String lines[] = sqlCreate.split("\\r?\\n");
		// Split the first line to insert the external keyword.
		String[] firstLine = lines[0].split("\\s+");
		// The new line should have external inserted.
		String firstLineNew = "";
		if (external)
			firstLineNew = firstLine[0] + " external " + firstLine[1] + " " + firstLine[2] + suffix + " \n";
		else
			firstLineNew = firstLine[0] + " " + firstLine[1] + " " + firstLine[2] + suffix + " \n";
		// Add all of the lines in the original SQL to the first line, except those
		// which contain the primary key statement (if any) and the closing parenthesis.
		// For the last column statement, remove the final comma.
		StringBuilder builder = new StringBuilder(firstLineNew);
		int tail = hasPrimaryKey ? 3 : 2;
		for (int i = 1; i < lines.length - tail; i++) {
			if( checkPartitionKey && this.partition && 
				Arrays.asList(Partitioning.tables).contains(tableName) &&
				lines[i].contains(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)])) {
					continue;
			}
			else
				builder.append(lines[i] + "\n");
		}
		// Change the last comma for a space (since the primary key statement was
		// removed).
		char[] commaLineArray = lines[lines.length - tail].toCharArray();
		commaLineArray[commaLineArray.length - 1] = ' ';
		builder.append(new String(commaLineArray) + "\n");
		// Close the parenthesis.
		builder.append(") \n");
		// Version 2.1 of Hive does not recognize integer, so use int instead.
		return builder.toString().replace("integer", "int    ");
	}

	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Hive.
	private String externalCreateTableHive(String incompleteSqlCreate, String tableName, String rawDataDir,
			Optional<String> extTablePrefixRaw) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		String fieldTerminatedBy = "'\001'";
		if( this.columnDelimiter.equals("PIPE") )
			fieldTerminatedBy = "'|'";
		builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY " + fieldTerminatedBy + " \n");
		builder.append("STORED AS TEXTFILE \n");
		if( extTablePrefixRaw.isPresent() )
			builder.append("LOCATION '" + extTablePrefixRaw.get() + "/" + tableName + "' \n");
		else
			builder.append("LOCATION '" + rawDataDir + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an external textfile table in Presto.
	private String externalCreateTablePresto(String incompleteSqlCreate, String tableName, String rawDataDir,
			Optional<String> extTablePrefixRaw) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		builder.append("WITH ( format = 'TEXTFILE', \n");
		if( extTablePrefixRaw.isPresent() )
			builder.append("external_location = '" + extTablePrefixRaw.get() + "/" + tableName + "' ) \n");
		else
			builder.append("external_location = '" + rawDataDir + "/" + tableName + "' ) \n");
		return builder.toString();
	}
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	private String internalCreateTableHive(String incompleteSqlCreate, String tableName,
			Optional<String> extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		//Add the partition statement, if needed.
		if( this.partition ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
			if( pos != -1 )
				//Use for Hive format.
				builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + " integer) \n" );
		}
		else if( this.bucketing ) {
			int pos = Arrays.asList(Bucketing.tables).indexOf(tableName);
			if( pos != -1 )
				//Use for Hive format.
				builder.append("CLUSTERED BY (" + Bucketing.bucketKeys[pos] + ") INTO " +
						Bucketing.bucketCount[pos] + " BUCKETS \n" );
		}
		// Add the stored as statement.
		if( extTablePrefixCreated.isPresent() ) {
			if( format.equals("parquet") )
				builder.append("STORED AS PARQUET \n");
			else if( format.equals("orc") )
				builder.append("STORED AS ORC \n");
			builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
			if( format.equals("parquet") )
				builder.append("TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
			else if( format.equals("orc") )
				builder.append("TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\") \n");
		}
		else {
			if( format.equals("parquet") )
				builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
			else if( format.equals("orc") )
				builder.append("STORED AS ORC TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\") \n");
		}
		return builder.toString();
	}
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Presto.
	private String internalCreateTablePresto(String incompleteSqlCreate, String tableName,
			Optional<String> extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( format.equals("parquet") ) {
			builder.append("WITH ( format = 'PARQUET' \n");
		}
		else if( format.equals("orc") ) {
			builder.append("WITH ( format = 'ORC' \n");
		}
		if( extTablePrefixCreated.isPresent() )
			builder.append(", external_location = '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		if( this.partition ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
			if( pos != -1 ) {
				builder.append(", partitioned_by = ARRAY['" + Partitioning.partKeys[pos] + "'] \n");
			}
		}
		builder.append(") \n");
		return builder.toString();
	}

	private String internalCreateTableDatabricks(String incompleteSqlCreate, String tableName,
			Optional<String> extTablePrefixCreated, String format) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( extTablePrefixCreated.isPresent() ) {
			if( format.equalsIgnoreCase("DELTA") )
				builder.append("USING DELTA \n");
			else
				//builder.append("USING org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat \n");
				builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			if( this.partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n" );
			}
			//builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		}
		else {
			builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			if( this.partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					//Use for Hive format.
					//builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + " integer) \n" );
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n");
			}
			//Use for Hive format.
			//builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
		}
		return builder.toString();
	}
	
	private String createPartitionInsertStmt(String tableName, List<String> columns, String suffix, String insertVariant) {
		StringBuilder builder = new StringBuilder();
		builder.append(insertVariant + " SELECT \n");
		for(String column : columns) {
			if ( column.equalsIgnoreCase(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] ))
				continue;
			else
				builder.append(column + ", \n");
		}
		builder.append(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + " \n");
		builder.append("FROM " + tableName + suffix + "\n");
		return builder.toString();
	}
	
	private List<String> extractColumnNames(String sqlStr) {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
				continue;
				if( line.trim().startsWith("create") || line.trim().startsWith("(") || line.trim().startsWith(")") )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				list.add(tokenizer.nextToken());
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return list;
	}
	
	private String shiftPartitionColumn(String sqlStr, String partitionAtt) {
		ArrayList<String> list = new ArrayList<String>();
		String partitionAttLine = null;
		BufferedReader reader = null;
		StringBuilder builder = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
					continue;
				if( line.contains(partitionAtt) ) {
					partitionAttLine = line;
					continue;
				}
				list.add(line);
			}
			//First remove the comma at the end in the partition attribute line.
			partitionAttLine = partitionAttLine.replace(',', ' ');
			//And add the comma at the end of the current last attribute.
			String lastAttLine = list.get(list.size() - 2);
			list.set(list.size() - 2, lastAttLine + ",");
			//Now insert the partition attribute line at the next to last position.
			list.add(list.size() - 1 ,partitionAttLine);
			builder = new StringBuilder();
			for(String s : list)
				builder.append(s + "\n");
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return builder.toString();
	}
	
	public void saveCreateTableFile(String suffix, String tableName, String sqlCreate) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + this.createTableDir +
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
	
	private void createSnowflakeStageQuery(String stageName) {
		try {
			Statement stmt = con.createStatement();
			stmt.execute("CREATE STAGE " + stageName + 
					" storage_integration = s3_integration url = '" + 
					this.extTablePrefixRaw.get() + "'");
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}
	
	private void countRowsQuery(Statement stmt, String tableName) {
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
	
	public String readFileContents(String filename) {
		BufferedReader inBR = null;
		String retVal = null;
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line = null;
			StringBuilder builder = new StringBuilder();
			while ((line = inBR.readLine()) != null) {
				builder.append(line + "\n");
			}
			retVal = builder.toString();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return retVal;
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


