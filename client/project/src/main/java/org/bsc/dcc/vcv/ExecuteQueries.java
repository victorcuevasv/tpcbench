package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.sql.DriverManager;
import java.io.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.StringTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.FieldValueList;

public class ExecuteQueries {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private static final String redshiftDriverName = "com.amazon.redshift.jdbc42.Driver";
	private static final String synapseDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private Connection con;
	private BigQueryDAO bigQueryDAO;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final String queriesDir;
	private final String resultsSubDir;
	private final String plansSubDir;
	private final boolean savePlans;
	private final boolean saveResults;
	private final String hostname;
	private final String jarFile;
	private final String querySingleOrAll;
	private final boolean useCachedResultSnowflake = false;
	private final boolean saveSnowflakeHistory = false;
	private final String clusterId;
	private final String userId;
	private final int runs;
	private final String dbPassword;
	
	
	public ExecuteQueries(CommandLine commandLine) {
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "power");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.queriesDir = commandLine.getOptionValue("queries-dir-in-jar", "QueriesSpark");
		this.resultsSubDir = commandLine.getOptionValue("results-subdir", "results");
		this.plansSubDir = commandLine.getOptionValue("plans-subdir", "plans");
		String savePlansStr = commandLine.getOptionValue("save-power-plans", "true");
		this.savePlans = Boolean.parseBoolean(savePlansStr);
		String saveResultsStr = commandLine.getOptionValue("save-power-results", "true");
		this.saveResults = Boolean.parseBoolean(saveResultsStr);
		this.jarFile = commandLine.getOptionValue("jar-file");
		this.hostname = commandLine.getOptionValue("server-hostname");
		//If running the zorder test, force the execution of all queries
		if( this.test.equals("zorder") )
			this.querySingleOrAll = "all";
		else
			this.querySingleOrAll = commandLine.getOptionValue("all-or-query-file");
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.userId = commandLine.getOptionValue("connection-username", "UNUSED");
		this.dbPassword = commandLine.getOptionValue("db-password", "UNUSED");
		String runsStr = commandLine.getOptionValue("power-test-runs", "1");
		this.runs = Integer.parseInt(runsStr);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
				this.system, this.test, this.instance);
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
	 * args[5] test name (e.g. power)
	 * args[6] experiment instance number
	 * args[7] queries dir
	 * args[8] subdirectory of work directory to store the results
	 * args[9] subdirectory of work directory to store the execution plans
	 * 
	 * args[10] save plans (boolean)
	 * args[11] save results (boolean)
	 * args[12] hostname of the server
	 * args[13] jar file
	 * args[14] "all" or query file
	 * 
	 */
	// Deprecated. No longer able to set up all of the options.
	public ExecuteQueries(String[] args) {
		if( args.length != 15 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Insufficient arguments: " + args.length);
			System.exit(1);
		}
		this.workDir = args[0];
		this.dbName = args[1];
		this.resultsDir = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.queriesDir = args[7];
		this.resultsSubDir = args[8];
		this.plansSubDir = args[9];
		this.savePlans = Boolean.parseBoolean(args[10]);
		this.saveResults = Boolean.parseBoolean(args[11]);
		this.hostname = args[12];
		this.jarFile = args[13];
		//If running the zorder test, force the execution of all queries
		if( this.test.equals("zorder") )
			this.querySingleOrAll = "all";
		else
			this.querySingleOrAll = args[14];
		this.clusterId = "UNUSED";
		this.userId = "UNUSED";
		this.dbPassword = "UNUSED";
		this.runs = 1;
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorder(this.workDir, this.resultsDir, this.experimentName,
						this.system, this.test, this.instance);
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
				setPrestoDefaultSessionOpts();
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
				this.setSnowflakeDefaultSessionOpts();
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
	

	private void setPrestoDefaultSessionOpts() {
		((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
		((PrestoConnection)con).setSessionProperty("join_reordering_strategy", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("join_distribution_type", "AUTOMATIC");
		((PrestoConnection)con).setSessionProperty("task_concurrency", "16");
		((PrestoConnection)con).setSessionProperty("spill_enabled", "false");
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
	
	
	private void setSnowflakeQueryTag(String tag) {
		try {
			Statement sessionStmt = this.con.createStatement();
			sessionStmt.executeUpdate("ALTER SESSION SET QUERY_TAG = '" + tag + "'");
			sessionStmt.close();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in setSnowflakeQueryTag");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private String createSnowflakeHistoryFileAndColumnList(String fileName) throws Exception {
		File tmp = new File(fileName);
		tmp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(fileName, false);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		String[] titles = {"QUERY_ID", "QUERY_TEXT", "DATABASE_NAME", "SCHEMA_NAME", "QUERY_TYPE",
				"SESSION_ID", "USER_NAME", "ROLE_NAME", "WAREHOUSE_NAME", "WAREHOUSE_SIZE",
				"WAREHOUSE_TYPE", "CLUSTER_NUMBER", "QUERY_TAG", "EXECUTION_STATUS", "ERROR_CODE",
				"ERROR_MESSAGE", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME", "BYTES_SCANNED",
				"ROWS_PRODUCED", "COMPILATION_TIME", "EXECUTION_TIME", "QUEUED_PROVISIONING_TIME",
				"QUEUED_REPAIR_TIME", "QUEUED_OVERLOAD_TIME", "TRANSACTION_BLOCKED_TIME", 
				"OUTBOUND_DATA_TRANSFER_CLOUD", "OUTBOUND_DATA_TRANSFER_REGION", 
				"OUTBOUND_DATA_TRANSFER_BYTES", "INBOUND_DATA_TRANSFER_CLOUD", 
				"INBOUND_DATA_TRANSFER_REGION", "INBOUND_DATA_TRANSFER_BYTES", "CREDITS_USED_CLOUD_SERVICES"};
		StringBuilder headerBuilder = new StringBuilder();
		StringBuilder columnsBuilder = new StringBuilder();
		for(int i = 0; i < titles.length; i++) {
			if( ! titles[i].equals("QUERY_TEXT") ) {
				if( i < titles.length - 1) {
					headerBuilder.append(String.format("%-30s|", titles[i]));
					columnsBuilder.append(titles[i] + ",");
				}
				else {
					headerBuilder.append(String.format("%-30s", titles[i]));
					columnsBuilder.append(titles[i]);
				}
			}
		}
		printWriter.println(headerBuilder.toString());
		printWriter.close();
		return columnsBuilder.toString();
	}
	
	
	private void saveSnowflakeHistory() {
		try {
			String historyFile = this.workDir + "/" + this.resultsDir + "/analytics/" + 
					this.experimentName + "/" + this.test + "/" + this.instance + "/history.log";
			String columnsStr = this.createSnowflakeHistoryFileAndColumnList(historyFile);
			this.setSnowflakeQueryTag("saveHistory");
			Statement historyStmt = this.con.createStatement();
			String historySQL = "select " + columnsStr + " " + 
			"from table( " + 
			"information_schema.query_history_by_session(CAST(CURRENT_SESSION() AS INTEGER), NULL, NULL, 10000)) " +
			"where query_type = 'SELECT' AND query_tag <> 'saveHistory' " +
			"order by end_time;";
			ResultSet rs = historyStmt.executeQuery(historySQL);
			this.saveResults(historyFile, rs, true);
			historyStmt.close();
			this.setSnowflakeQueryTag("");
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in saveSnowflakeHistory");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	public boolean getSaveSnowflakeHistory() {
		return this.saveSnowflakeHistory;
	}
	
	private void prepareRedshift() {
		try {
			System.out.print("Disabling result caching...");
			Statement stmt = con.createStatement();
			stmt.execute("SET enable_result_cache_for_session TO off");
			System.out.println("done");
		} catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error when disabling results caching");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	private void prepareDatabricksSql() {
		try {
			System.out.print("Disabling result caching...");
			Statement stmt = con.createStatement();
			stmt.execute("SET spark.databricks.execution.resultCaching.enabled=false;");
			System.out.println("done");
		} catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error when disabling results caching");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private void prepareSnowflake() {
		this.useDatabaseQuery(this.dbName);
		this.useSchemaQuery(this.dbName);
		this.useSnowflakeWarehouseQuery(this.clusterId);	
	}
	
	public static void main(String[] args) {
		ExecuteQueries application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new ExecuteQueries(args);
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
				logger.error("Error in ExecuteQueriesSpark main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new ExecuteQueries(commandLine);
		}
		application.executeQueries();
	}
	
	
	public void executeQueries() {
		if( this.system.startsWith("snowflake") )
			this.prepareSnowflake();
		// If the system is Redshift disable query result caching
		if (this.system.startsWith("redshift"))
			this.prepareRedshift();
		//if (this.system.startsWith("databrickssql"))
			//this.prepareDatabricksSql();
		if( this.test.equals("power") && this.savePlans )
			this.savePlans();
		this.recorder.header();
		for(int i = 1; i <= this.runs; i++) {
			for (final String fileName : this.queriesReader.getFilesOrdered()) {
				if( ! this.querySingleOrAll.equals("all") ) {
					if( ! fileName.equals(this.querySingleOrAll) )
						continue;
				}
				String sqlStr = this.queriesReader.getFile(fileName);
				String nQueryStr = fileName.replaceAll("[^\\d]", "");
				int nQuery = Integer.parseInt(nQueryStr);
				if( this.system.equals("prestoemr") )
					this.setPrestoDefaultSessionOpts();
				QueryRecord queryRecord = null;
				if( this.system.equals("bigquery") )
					queryRecord = new QueryRecordBigQuery(nQuery);
				else
					queryRecord = new QueryRecord(nQuery);
				queryRecord.setRun(i);
				this.logger.info("\nExecuting query: " + fileName + " (run " + i + ")\n" + sqlStr);
				try {
					if( ! this.system.equals("bigquery") )
						this.executeQueryMultipleCalls(fileName, sqlStr, queryRecord);
					else
						this.executeQueryMultipleCallsBigQuery(fileName, sqlStr, queryRecord);
					//The results should have been saved by executeQueryMultipleCalls,
					//check the generated file to obtain the results size.
					if( this.saveResults ) {
						String queryResultsFileName = this.generateResultsFileName(fileName, i);
						File resultsFile = new File(queryResultsFileName);
						queryRecord.setResultsSize(resultsFile.length());
					}
					else
						queryRecord.setResultsSize(0);
					queryRecord.setSuccessful(true);
				}
				catch(Exception e) {
					e.printStackTrace();
					this.logger.error("Error processing: " + fileName);
					this.logger.error(e);
					this.logger.error(AppUtil.stringifyStackTrace(e));
				}
				finally {
					queryRecord.setEndTime(System.currentTimeMillis());
					this.recorder.record(queryRecord);
				}
			}
		}
		this.recorder.close();
		if( this.system.startsWith("snowflake") && this.saveSnowflakeHistory )
			this.saveSnowflakeHistory();
		//Close the connection if using redshift as the driver leaves threads on the background
		//that prevent the application from closing. 
		if (this.system.equals("redshift") || this.system.equals("synapse"))
			this.closeConnection();
	}
	
	
	private void savePlans() {
		for (final String fileName : this.queriesReader.getFilesOrdered()) {
			if( ! this.querySingleOrAll.equals("all") ) {
				if( ! fileName.equals(this.querySingleOrAll) )
					continue;
			}
			String sqlStrFull = this.queriesReader.getFile(fileName);
			String nQueryStr = fileName.replaceAll("[^\\d]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			this.logger.info("Generating query plan for: " + fileName);
			System.out.println("Generating query plan for: " + fileName);
			// Split the various queries and obtain the plan for each.
			StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
			boolean firstQuery = true;
			while (tokenizer.hasMoreTokens()) {
				String sqlStr = tokenizer.nextToken().trim();
				if( sqlStr.length() == 0 )
					continue;	
				try {
					Statement stmt = this.con.createStatement();
					String explainStr = "EXPLAIN ";
					if( this.system.startsWith("presto") )
						explainStr += "(FORMAT GRAPHVIZ) ";
					ResultSet planRS = stmt.executeQuery(explainStr + sqlStr);
					this.saveResults(this.generatePlansFileName(fileName), planRS, ! firstQuery);
					planRS.close();
					stmt.close();
					firstQuery = false;
				}
				catch(Exception e) {
					e.printStackTrace();
					this.logger.error("Error generating plan for: " + fileName);
					this.logger.error(e);
					this.logger.error(AppUtil.stringifyStackTrace(e));
				}
			}
		}
	}
	
	
	private String generateResultsFileName(String queryFileName, int run) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		noExtFileName += ("_" + run);
		return this.workDir + "/" + this.resultsDir + "/" + this.resultsSubDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	private String generatePlansFileName(String queryFileName) {
		String noExtFileName = queryFileName.substring(0, queryFileName.indexOf('.'));
		return this.workDir + "/" + this.resultsDir + "/" + this.plansSubDir + "/" + this.experimentName + 
				"/" + this.test + "/" + this.instance + "/" + noExtFileName + ".txt";
	}
	
	
	// Execute the queries from the provided file.
	private void executeQueryMultipleCalls(String queryFileName, String sqlStrFull, QueryRecord queryRecord) 
			throws Exception {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;	
			if( sqlStr.contains("SET SESSION") ) {
				Statement sessionStmt = con.createStatement();
				sessionStmt.executeUpdate(sqlStr);
				sessionStmt.close();
				continue;
			}
			// Execute the query.
			Statement stmt = con.createStatement();
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + 
				queryFileName + " (run " + queryRecord.getRun() + ").");
			if( this.system.startsWith("snowflake") )
				this.setSnowflakeQueryTag("q" + queryRecord.getQuery() + "_i" + 
						iteration + "_r" + queryRecord.getRun());
			ResultSet rs = stmt.executeQuery(sqlStr);
			if( this.system.startsWith("snowflake") )
				this.setSnowflakeQueryTag("");
			// Save the results.
			if( this.test.equals("power") &&  this.saveResults ) {
				int tuples = this.saveResults(
						this.generateResultsFileName(queryFileName, queryRecord.getRun()), rs, ! firstQuery);
				queryRecord.setTuples(queryRecord.getTuples() + tuples);
			}
			stmt.close();
			rs.close();
			firstQuery = false;
			iteration++;
		}
	}
	
	private void executeQueryMultipleCallsBigQuery(String queryFileName, String sqlStrFull, 
		QueryRecord queryRecord) throws Exception {
		// Split the various queries and execute each.
		StringTokenizer tokenizer = new StringTokenizer(sqlStrFull, ";");
		boolean firstQuery = true;
		int iteration = 1;
		while (tokenizer.hasMoreTokens()) {
			String sqlStr = tokenizer.nextToken().trim();
			if( sqlStr.length() == 0 )
				continue;	
			// Execute the query.
			if( firstQuery )
				queryRecord.setStartTime(System.currentTimeMillis());
			System.out.println("Executing iteration " + iteration + " of query " + 
				queryFileName + " (run " + queryRecord.getRun() + ").");
			TableResult tableResult = this.bigQueryDAO.executeQuery(sqlStr, 
					(QueryRecordBigQuery)queryRecord);
			// Save the results.
			if( this.test.equals("power") &&  this.saveResults ) {
				int tuples = this.saveResultsBigQuery(
						this.generateResultsFileName(queryFileName, queryRecord.getRun()), 
							tableResult, ! firstQuery);
				queryRecord.setTuples(queryRecord.getTuples() + tuples);
			}
			firstQuery = false;
			iteration++;
		}
	}

	private int saveResults(String resFileName, ResultSet rs, boolean append) 
			throws Exception {
		File tmp = new File(resFileName);
		tmp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(resFileName, append);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		ResultSetMetaData metadata = rs.getMetaData();
		int nCols = metadata.getColumnCount();
		int tuples = 0;
		while (rs.next()) {
			StringBuilder rowBuilder = new StringBuilder();
			for (int i = 1; i <= nCols - 1; i++) {
				rowBuilder.append(rs.getString(i) + " | ");
			}
			rowBuilder.append(rs.getString(nCols));
			printWriter.println(rowBuilder.toString());
			tuples++;
		}
		printWriter.close();
		return tuples;
	}
	
	private int saveResultsBigQuery(String resFileName, TableResult tableResult, boolean append) 
			throws Exception {
		File tmp = new File(resFileName);
		tmp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(resFileName, append);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		int nCols = tableResult.getSchema().getFields().size();
		int tuples = 0;
		for (FieldValueList row : tableResult.iterateAll()) {
			StringBuilder rowBuilder = new StringBuilder();
			String val = null;
			for (int i = 0; i < nCols - 1; i++) {
				val = null;
				if( ! row.get(i).isNull() )
					val = row.get(i).getStringValue();
				rowBuilder.append(val + " | ");
			}
			val = null;
			if( ! row.get(nCols - 1).isNull() )
				val = row.get(nCols - 1).getStringValue();
			rowBuilder.append(val);
			printWriter.println(rowBuilder.toString());
			tuples++;
		}
		printWriter.close();
		return tuples;
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
	
	private void useDatabaseQuery(String dbName) {
		try {
			Statement sessionStmt = con.createStatement();
			sessionStmt.executeUpdate("USE DATABASE " + dbName);
			sessionStmt.close();
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
	
	private void closeConnection() {
		try {
			this.con.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

}
