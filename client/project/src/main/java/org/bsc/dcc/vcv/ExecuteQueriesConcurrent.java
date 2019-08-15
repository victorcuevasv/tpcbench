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

	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private static final String databricksDriverName = "com.simba.spark.jdbc41.Driver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
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
	final private int nStreams;
	final private long seed;
	final private boolean multiple;

	
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
	 * args[13] number of streams
	 * args[14] random seed
	 * 
	 * args[15] use multiple connections (true|false)
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
		this.nStreams = Integer.parseInt(args[13]);
		this.seed = Long.parseLong(args[14]);
		this.multiple = Boolean.parseBoolean(args[15]);
		this.random = new Random(seed);
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
			else if( system.equals("sparkdatabricksjdbc") ) {
				Class.forName(databricksDriverName);
				con = DriverManager.getConnection("jdbc:hive2://hostname:443/" + dbName);
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/" + dbName, "hive", "");
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

	
	public static void main(String[] args) throws SQLException {
		if( args.length != 16 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		ExecuteQueriesConcurrent prog = new ExecuteQueriesConcurrent(args);
		prog.doRun();
	}
	
	
	private void doRun() {
		File directory = new File(this.workDir + "/" + this.queriesDir);
		// Process each .sql file found in the directory.
		// The preprocessing steps are necessary to obtain the right order, i.e.,
		// query1.sql, query2.sql, query3.sql, ..., query99.sql.
		File[] files = Stream.of(directory.listFiles()).
				map(File::getName).
				map(AppUtil::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				map(s -> new File(this.workDir + "/" + this.queriesDir + "/" + s)).
				toArray(File[]::new);
		HashMap<Integer, String> queriesHT = this.createQueriesHT(files);
		int nQueries = files.length;
		this.executeStreams(nQueries, queriesHT);
	}
	
	
	public void executeStreams(int nQueries, HashMap<Integer, String> queriesHT) {
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
	
	
	public HashMap<Integer, String> createQueriesHT(File[] files) {
		HashMap<Integer, String> queriesHT = new HashMap<Integer, String>();
		for(File file : files) {
			int nQuery = AppUtil.extractNumber(file.getName());
			String sqlStr = readFileContents(file.getAbsolutePath());
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

