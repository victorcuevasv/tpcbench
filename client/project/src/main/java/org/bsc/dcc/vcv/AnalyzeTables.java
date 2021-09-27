package org.bsc.dcc.vcv;

import java.io.*;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class AnalyzeTables {

	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private static final String redshiftDriverName = "com.amazon.redshift.jdbc42.Driver";
	private static final String synapseDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final AnalyticsRecorder recorder;
	private final JarCreateTableReaderAsZipFile analyzeTableReader;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final boolean computeForCols;
	private final String hostname;
	private final String jarFile;
	private final String createTableDir;
	private final String createSingleOrAll;
	private String systemRunning;
	private final String clusterId;
	private final String userId;
	private final String dbPassword;
	
	
	public AnalyzeTables(CommandLine commandLine) {
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "analyze");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		String computeForColsStr = commandLine.getOptionValue("use-column-stats");
		this.computeForCols = Boolean.parseBoolean(computeForColsStr);
		this.hostname = commandLine.getOptionValue("server-hostname");
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.createTableDir = commandLine.getOptionValue("create-table-dir", "tables");
		this.createSingleOrAll = commandLine.getOptionValue("all-or-create-file", "all");
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.dbPassword = commandLine.getOptionValue("db-password", "UNUSED");
		this.userId = commandLine.getOptionValue("connection-username", "UNUSED");
		this.analyzeTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.systemRunning = this.system;
		if( commandLine.hasOption("override-analyze-system") ) {
			this.systemRunning = commandLine.getOptionValue("override-analyze-system");
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
	 * args[7] compute statistics for columns (true/false)
	 * args[8] hostname of the server
	 * args[9] jar file
	 * 
	 * args[10] subdirectory within the jar that contains the create table files
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public AnalyzeTables(String[] args) {
		if( args.length != 11 ) {
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
		this.computeForCols = Boolean.parseBoolean(args[7]);
		this.hostname = args[8];
		this.jarFile = args[9];
		this.createTableDir = args[10];
		this.createSingleOrAll = "all";
		this.clusterId = args[26];
		this.userId = "UNUSED";
		this.dbPassword = "UNUSED";
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
		this.analyzeTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.createTableDir);
		this.systemRunning = this.system;
		this.openConnection();
	}

	
	private void openConnection() {
		try {
			String driverName = "";
			if( this.system.equals("hive") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10000/" + this.dbName, "hive", "");
			}
			else if( this.system.equals("presto") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8080/hive/" + this.dbName, "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( this.system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hive", "");
			}
			else if( this.system.equals("sparkdatabricksjdbc") ) {
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
					+ ";spark.databricks.execution.resultCaching.enabled=false"
					+ ";spark.databricks.adaptive.autoOptimizeShuffle.enabled=false"
					+ ";spark.sql.shuffle.partitions=2048"
					// + ";spark.sql.autoBroadcastJoinThreshold=60000000"
					);
			}
			else if( this.system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			else if( this.system.startsWith("snowflake") ) {
				String snowflakePwd = AWSUtil.getValue("SnowflakePassword");
				Class.forName(snowflakeDriverName);
				this.con = DriverManager.getConnection("jdbc:snowflake://" + 
						this.hostname + "/?" +
						"user=" + this.userId + "&password=" + snowflakePwd +
						"&warehouse=" + this.clusterId + "&schema=" + this.dbName);
			}
			else if( this.system.equals("redshift") ) {
				Class.forName(redshiftDriverName);
				//Use Synapse's password temporarily (must be specified when creating the cluster)
				String redshiftPwd = AWSUtil.getValue("SynapsePassword");
				this.con = DriverManager.getConnection("jdbc:redshift://" + this.hostname + ":5439/" +
				this.dbName + "?ssl=true&UID=" + this.userId + "&PWD=" + redshiftPwd);
			}
            else if( this.system.startsWith("synapse") ) {
                String synapsePwd = AWSUtil.getValue("SynapsePassword");
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
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	public static void main(String[] args) {
		AnalyzeTables application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new AnalyzeTables(args);
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
				logger.error("Error in AnalyzeTables main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new AnalyzeTables(commandLine);
		}
		application.analyzeTables();
		//prog.closeConnection();
	}
	
	
	private void analyzeTables() {
		//if( this.systemRunning.equals("hive") )
			//this.configureMapreduce();
		// Process each .sql create table file found in the jar file.
		this.recorder.header();
		List<String> unorderedList = this.analyzeTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		int i = 1;
		for (final String fileName : orderedList) {
			String sqlCreate = this.analyzeTableReader.getFile(fileName);
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
			if( this.systemRunning.startsWith("spark") || this.systemRunning.startsWith("databrickssql"))
				this.executeAnalyzeTableSpark(fileName, sqlCreate, i);
			else
				this.executeAnalyzeTable(fileName, sqlCreate, i);
			i++;
		}
		this.recorder.close();
		//Close the connection if using redshift as the driver leaves threads on the background
		//that prevent the application from closing. 
		if (this.system.equals("redshift") || this.system.equals("synapse"))
			this.closeConnection();
	}

	
	private void configureMapreduce() {
		String[] stmtStrs = {"set mapreduce.map.memory.mb=4096",
						  "set mapreduce.map.java.opts=-Xmx3686m",
						  "set mapreduce.reduce.memory.mb=4096",
						  "set mapreduce.reduce.java.opts=-Xmx3686m"
						  };
		try {
			Statement stmt = con.createStatement();
			for(String stmtStr: stmtStrs)
				stmt.executeUpdate(stmtStr);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void executeAnalyzeTable(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			queryRecord = new QueryRecord(index);
			System.out.println("\nAnalyzing table: " + tableName + "\n");
			this.logger.info("\nAnalyzing table: " + tableName + "\n");
			Statement stmt = this.con.createStatement();
			String sqlStr = null;
			queryRecord.setStartTime(System.currentTimeMillis());
			if( this.systemRunning.startsWith("presto") || this.systemRunning.equals("redshift")) {
				sqlStr = "ANALYZE " + tableName;
				this.saveAnalyzeTableFile("analyze", tableName, sqlStr);
				stmt.executeUpdate(sqlStr);
			}
			if( this.systemRunning.equals("hive") && this.computeForCols ) {
				//sqlStr = "ANALYZE TABLE " + tableName + " COMPUTE STATISTICS";
				//stmt.executeUpdate(sqlStr);
				//ResultSet rs = stmt.executeQuery("DESCRIBE " + tableName);
				//String columnsStr = extractColumns(rs, 1);
				String columnsStr = extractColumnNames(sqlCreate);
				String sqlStrCols = "ANALYZE TABLE " + tableName + 
						" COMPUTE STATISTICS FOR COLUMNS " + columnsStr;
				this.saveAnalyzeTableFile("analyze", tableName, sqlStrCols);
				stmt.executeUpdate(sqlStrCols);
			}
			queryRecord.setSuccessful(true);
			stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.recorder.record(queryRecord);
		}
	}
	
	
	private void executeAnalyzeTableSpark(String sqlCreateFilename, String sqlCreate, int index) {
		QueryRecord queryRecord = null;
		try {
			String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
			System.out.println("Analyzing table: " + tableName);
			this.logger.info("Analyzing table: " + tableName);
			// Skip the dbgen_version table since its time attribute is not
			// compatible with Hive.
			if (tableName.equals("dbgen_version")) {
				System.out.println("Skipping: " + tableName);
				return;
			}
			queryRecord = new QueryRecord(index);
			queryRecord.setStartTime(System.currentTimeMillis());
			Statement stmt = con.createStatement();
			if( this.computeForCols ) {
				String sqlStrCols = "";
				if (this.systemRunning.startsWith("databrickssql")) 
					sqlStrCols = "ANALYZE TABLE " + tableName + " COMPUTE STATISTICS FOR ALL COLUMNS;";
				else {
					//ResultSet rs = stmt.executeQuery("DESCRIBE " + tableName);
					//String columnsStr = extractColumns(rs, 0);
					String columnsStr = extractColumnNames(sqlCreate);
					sqlStrCols = "ANALYZE TABLE " + tableName + " COMPUTE STATISTICS FOR COLUMNS " + 
							columnsStr;
				}
				this.saveAnalyzeTableFile("analyze", tableName, sqlStrCols);
				stmt.executeUpdate(sqlStrCols);
			}
			else {
				String sqlStr = "ANALYZE TABLE " + tableName + " COMPUTE STATISTICS";
				this.saveAnalyzeTableFile("analyze", tableName, sqlStr);
				stmt.executeUpdate(sqlStr);
			}
			queryRecord.setSuccessful(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in AnalyzeTables analyzeTableSpark.");
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
	
	
	private String extractColumnNames(String sqlStr) {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
				continue;
				if( line.trim().startsWith("create") || line.trim().startsWith("(") 
						|| line.trim().startsWith(")") || line.trim().startsWith("primary key") )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				list.add(tokenizer.nextToken());
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return list.stream().collect( Collectors.joining( ", " ) );
	}
	
	
	public void saveAnalyzeTableFile(String suffix, String tableName, String sqlAnalyze) {
		try {
			String analyzeTableFileName = this.workDir + "/" + this.resultsDir + "/" + this.createTableDir +
											suffix + "/" + this.experimentName + "/" + this.instance +
											"/" + tableName + ".sql";
			File temp = new File(analyzeTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(analyzeTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(sqlAnalyze);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}
	
	
	private void closeConnection() {
		try {
			this.con.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	
}


