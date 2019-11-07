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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class ExecuteQueriesConcurrent implements ConcurrentExecutor {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc41.Driver";
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private Connection con;
	private final JarQueriesReaderAsZipFile queriesReader;
	private final AnalyticsRecorderConcurrent recorder;
	private final ExecutorService executor;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 100;
	private final Random random;
	final String workDir;
	final String dbName;
	final String folderName;
	final String experimentName;
	final String system;
	final String test;
	final int instance;
	final String queriesDir;
	final String resultsDir;
	final String plansDir;
	final boolean savePlans;
	final boolean saveResults;
	final private String hostname;
	final private String jarFile;
	final private int nStreams;
	final private long seed;
	final private boolean multiple;
	private final boolean useCachedResultSnowflake = false;

	
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
		this.workDir = args[0];
		this.dbName = args[1];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.queriesDir = args[7];
		this.resultsDir = args[8];
		this.plansDir = args[9];
		this.savePlans = Boolean.parseBoolean(args[10]);
		this.saveResults = Boolean.parseBoolean(args[11]);
		this.hostname = args[12];
		this.jarFile = args[13];
		this.nStreams = Integer.parseInt(args[14]);
		this.seed = Long.parseLong(args[15]);
		this.multiple = Boolean.parseBoolean(args[16]);
		this.random = new Random(seed);
		this.queriesReader = new JarQueriesReaderAsZipFile(this.jarFile, this.queriesDir);
		this.recorder = new AnalyticsRecorderConcurrent(this.workDir, this.folderName,
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
				Class.forName(databricksDriverName);
				this.con = DriverManager.getConnection("jdbc:spark://" + this.hostname + ":443/" +
				this.dbName + ";transportMode=http;ssl=1" + 
				";httpPath=sql/protocolv1/o/538214631695239/" + 
				"<cluster name>;AuthMech=3;UID=token;PWD=<personal-access-token>" +
				";UseNativeQuery=1");
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/" + dbName, "hive", "");
			}
			else if( system.startsWith("snowflake") ) {
				Class.forName(snowflakeDriverName);
				con = DriverManager.getConnection("jdbc:snowflake://" + this.hostname + "/?" +
						"user=bsctest" + "&password=c4[*4XYM1GIw" + "&db=" + this.dbName +
						"&schema=" + this.dbName + "&warehouse=testwh");
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
		((PrestoConnection)con).setSessionProperty("task_concurrency", "8");
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

	
	public static void main(String[] args) throws SQLException {
		if( args.length != 17 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		ExecuteQueriesConcurrent prog = new ExecuteQueriesConcurrent(args);
		prog.executeStreams();
	}
	
	
	private void executeStreams() {
		List<String> files = queriesReader.getFilesOrdered();
		HashMap<Integer, String> queriesHT = createQueriesHT(files, this.queriesReader);
		int nQueries = files.size();
		int totalQueries = nQueries * this.nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < this.nStreams; i++) {
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

}

