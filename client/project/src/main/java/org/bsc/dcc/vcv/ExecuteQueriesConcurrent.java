package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.sql.DriverManager;
import java.io.*;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

public class ExecuteQueriesConcurrent implements ConcurrentExecutor {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private Connection con;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final JarStreamsReaderAsZipFile streamsReader;
	private final AnalyticsRecorderConcurrent recorder;
	private final ExecutorService executor;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 150;
	private final Random random;
	final String workDir;
	final String dbName;
	final String resultsDir;
	final String experimentName;
	final String system;
	final String test;
	final int instance;
	final String queriesDir;
	final String resultsSubDir;
	final String plansSubDir;
	final boolean savePlans;
	final boolean saveResults;
	final private String hostname;
	final private String jarFile;
	final private int nStreams;
	final private long seed;
	final private boolean multiple;
	final int[][] matrix;
	final private boolean tputChangingStreams;
	private final boolean useCachedResultSnowflake = false;
	private final int maxConcurrencySnowflake = 8;
	private final String clusterId;
	private final String userId;
	
	public ExecuteQueriesConcurrent(CommandLine commandLine) {
		this.workDir = commandLine.getOptionValue("main-work-dir");
		this.dbName = commandLine.getOptionValue("schema-name");
		this.resultsDir = commandLine.getOptionValue("results-dir");
		this.experimentName = commandLine.getOptionValue("experiment-name");
		this.system = commandLine.getOptionValue("system-name");
		this.test = commandLine.getOptionValue("tpcds-test", "tput");
		String instanceStr = commandLine.getOptionValue("instance-number");
		this.instance = Integer.parseInt(instanceStr);
		this.queriesDir = commandLine.getOptionValue("queries-dir-in-jar", "QueriesSpark");
		this.resultsSubDir = commandLine.getOptionValue("results-subdir", "results");
		this.plansSubDir = commandLine.getOptionValue("plans-subdir", "plans");
		String savePlansStr = commandLine.getOptionValue("save-tput-plans", "false");
		this.savePlans = Boolean.parseBoolean(savePlansStr);
		String saveResultsStr = commandLine.getOptionValue("save-tput-results", "true");
		this.saveResults = Boolean.parseBoolean(saveResultsStr);
		this.jarFile = commandLine.getOptionValue("jar-file");
		String nStreamsStr = commandLine.getOptionValue("number-of-streams"); 
		this.nStreams = Integer.parseInt(nStreamsStr);
		String seedStr = commandLine.getOptionValue("random-seed", "1954"); 
		this.seed = Long.parseLong(seedStr);
		this.random = new Random(seed);
		String multipleStr = commandLine.getOptionValue("multiple-connections", "false"); 
		this.multiple = Boolean.parseBoolean(multipleStr);
		this.hostname = commandLine.getOptionValue("server-hostname");
		String tputChangingStreamsStr = commandLine.getOptionValue("tput-changing-streams", "true");
		this.tputChangingStreams = Boolean.parseBoolean(tputChangingStreamsStr);
		this.clusterId = commandLine.getOptionValue("cluster-id", "UNUSED");
		this.userId = commandLine.getOptionValue("connection-username", "UNUSED");
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.streamsReader = new JarStreamsReaderAsZipFile(this.jarFile, "streams");
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.resultsDir,
				this.experimentName, this.system, this.test, this.instance);
		this.matrix = this.streamsReader.getFileAsMatrix(this.streamsReader.getFiles().get(0));
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
		try {
			if( ! this.multiple )
				this.con = this.createConnection(this.system, this.hostname, this.dbName);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrent constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
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
	 * args[14] number of streams
	 * 
	 * args[15] random seed
	 * args[16] use multiple connections (true|false)
	 * 
	 */
	public ExecuteQueriesConcurrent(String[] args) {
		if( args.length != 17 ) {
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
		this.queriesDir = args[7];
		this.resultsSubDir = args[8];
		this.plansSubDir = args[9];
		this.savePlans = Boolean.parseBoolean(args[10]);
		this.saveResults = Boolean.parseBoolean(args[11]);
		this.hostname = args[12];
		this.jarFile = args[13];
		this.nStreams = Integer.parseInt(args[14]);
		this.seed = Long.parseLong(args[15]);
		this.multiple = Boolean.parseBoolean(args[16]);
		this.random = new Random(seed);
		this.tputChangingStreams = true;
		this.clusterId = "UNUSED";
		this.userId = "UNUSED";
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.streamsReader = new JarStreamsReaderAsZipFile(this.jarFile, "streams");
		this.matrix = this.streamsReader.getFileAsMatrix(this.streamsReader.getFiles().get(0));
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.resultsDir,
				this.experimentName, this.system, this.test, this.instance);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
		try {
			if( ! this.multiple )
				this.con = this.createConnection(this.system, this.hostname, this.dbName);
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrent constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public Connection createConnection(String system, String hostname, String dbName) {
		try {
			String driverName = "";
			if( system.equals("hive") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10000/" + dbName, "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/" + dbName, "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8889/hive/" + dbName, "hive", "");
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
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/" + dbName, "hive", "");
			}
			else if( system.startsWith("snowflake") ) {
				String snowflakePwd = AWSUtil.getValue("SnowflakePassword");
				Class.forName(snowflakeDriverName);
				this.con = DriverManager.getConnection("jdbc:snowflake://" + 
						this.hostname + "/?" +
						"user=" + this.userId + "&password=" + snowflakePwd +
						"&warehouse=" + this.clusterId + "&schema=" + this.dbName);
				this.setSnowflakeDefaultSessionOpts();
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		return con;
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
			sessionStmt.executeUpdate("ALTER WAREHOUSE SET MAX_CONCURRENCY_LEVEL = " + this.maxConcurrencySnowflake);
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
	
	
	public void saveSnowflakeHistory() {
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

	
	public static void main(String[] args) {
		ExecuteQueriesConcurrent application = null;
		//Check is GNU-like options are used.
		boolean gnuOptions = args[0].contains("--") ? true : false;
		if( ! gnuOptions )
			application = new ExecuteQueriesConcurrent(args);
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
				logger.error("Error in ExecuteQueriesConcurrent main.");
				logger.error(e);
				logger.error(AppUtil.stringifyStackTrace(e));
				System.exit(1);
			}
			application = new ExecuteQueriesConcurrent(commandLine);
		}
		application.executeStreams();
	}
	
	
	private void executeStreams() {
		List<String> files = queriesReader.getFilesOrdered();
		int nQueries = files.size();
		int totalQueries = nQueries * this.nStreams;
		CountDownLatch latch = new CountDownLatch(1);
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this, latch);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < this.nStreams; i++) {
			HashMap<Integer, String> queriesHT = null;
			if( this.tputChangingStreams ) {
				JarQueriesReaderAsZipFile streamQueriesReader = 
					new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir + "Stream" + i + "/");
				List<String> filesStream = streamQueriesReader.getFilesOrdered();
				queriesHT = createQueriesHT(filesStream, streamQueriesReader);
			}
			else {
				queriesHT = createQueriesHT(files, this.queriesReader);
			}
			QueryStream stream = null;
			if( ! this.multiple ) {
				stream = new QueryStream(i, this.resultsQueue, this.con, queriesHT,
						nQueries, this.random, this);
			}
			else {
				Connection con = this.createConnection(this.system, this.hostname, this.dbName);
				stream = new QueryStream(i, this.resultsQueue, con, queriesHT,
						nQueries, this.random, this);
			}
			this.executor.submit(stream);
		}
		this.executor.shutdown();
		try {
            latch.await();
        }
		catch (InterruptedException e) {
            e.printStackTrace();
        }
	}
	
	
	public HashMap<Integer, String> createQueriesHT(List<String> files, JarQueriesReaderAsZipFile queriesReader) {
		HashMap<Integer, String> queriesHT = new HashMap<Integer, String>();
		for(String file : files) {
			int nQuery = ExecuteQueriesConcurrent.extractNumber(file);
			String sqlStr = queriesReader.getFile(file);
			queriesHT.put(nQuery, sqlStr);
		}
		return queriesHT;
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
	
	
	// Converts a string representing a filename like query12.sql to the integer 12.
	public static int extractNumber(String fileName) {
		String nStr = fileName.substring(0, fileName.indexOf('.')).replaceAll("[^\\d.]", "");
		return Integer.parseInt(nStr);
	}
	
	
	public void closeConnection() {
		try {
			this.con.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueriesConcurrent closeConnection.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	
	private void saveResults(String resFileName, ResultSet rs, boolean append) 
			throws Exception {
		File tmp = new File(resFileName);
		tmp.getParentFile().mkdirs();
		FileWriter fileWriter = new FileWriter(resFileName, append);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		ResultSetMetaData metadata = rs.getMetaData();
		int nCols = metadata.getColumnCount();
		while (rs.next()) {
			StringBuilder rowBuilder = new StringBuilder();
			for (int i = 1; i <= nCols - 1; i++) {
				rowBuilder.append(rs.getString(i) + " | ");
			}
			rowBuilder.append(rs.getString(nCols));
			printWriter.println(rowBuilder.toString());
		}
		printWriter.close();
	}
	
	
	public void incrementAtomicCounter() {
		
	}

	
}

