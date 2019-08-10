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
	private AnalyticsRecorderConcurrent recorder;
	private ExecutorService executor;
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 100;
	private long seed;
	private Random random;
	private String system;
	private String hostname;
	private boolean multiple = false;
	boolean savePlans;
	boolean saveResults;
	private String dbName;

	public ExecuteQueriesConcurrent(String system, String hostname, boolean multiple,
			boolean savePlans, boolean saveResults, String dbName, String folderName,
			String experimentName, String instanceStr) {
		try {
			this.savePlans = savePlans;
			this.saveResults = saveResults;
			this.system = system;
			this.hostname = hostname;
			this.dbName = dbName;
			this.multiple = multiple;
			if( ! this.multiple )
				this.con = this.createConnection(system, hostname, dbName);
			int instance = Integer.parseInt(instanceStr);
			this.recorder = new AnalyticsRecorderConcurrent("tput", this.system, folderName, 
					experimentName, instance);
			this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
			this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
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
			system = system.toLowerCase();
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
						hostname + ":10015/" + dbName, "", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
			System.exit(1);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
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

	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory that contains the queries
	 * args[2] subdirectory of work directory to store the results
	 * args[3] subdirectory of work directory to store the execution plans
	 * args[4] system to evaluate the queries (hive/presto)
	 * args[5] hostname of the server
	 * args[6] number of streams
	 * args[7] random seed
	 * args[8] use multiple connections (true|false)
	 * args[9] save plans (true|false)
	 * args[10] save results (true|false)
	 * args[11] database name
	 * args[12] results folder name (e.g. for Google Drive)
	 * args[13] experiment name (name of subfolder within the results folder
	 * args[14] experiment instance number
	 * 
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length != 15 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Insufficient arguments: " + args.length);
			System.exit(1);
		}
		boolean multiple = Boolean.parseBoolean(args[8]);
		boolean savePlans = Boolean.parseBoolean(args[9]);
		boolean saveResults = Boolean.parseBoolean(args[10]);
		ExecuteQueriesConcurrent prog = new ExecuteQueriesConcurrent(args[4], args[5], multiple,
				savePlans, saveResults, args[11], args[12], args[13], args[14]);
		File directory = new File(args[0] + "/" + args[1]);
		// Process each .sql file found in the directory.
		// The preprocessing steps are necessary to obtain the right order, i.e.,
		// query1.sql, query2.sql, query3.sql, ..., query99.sql.
		File[] files = Stream.of(directory.listFiles()).
				map(File::getName).
				map(AppUtil::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				map(s -> new File(args[0] + "/" + args[1] + "/" + s)).
				toArray(File[]::new);
		HashMap<Integer, String> queriesHT = prog.createQueriesHT(files);
		int nStreams = Integer.parseInt(args[6]);
		long seed = Long.parseLong(args[7]);
		prog.seed = seed;
		prog.random = new Random(seed);
		int nQueries = files.length;
		prog.executeStreams(nQueries, nStreams, prog.random, queriesHT,
				args[0], args[2], args[3], false);
	}
	
	public void executeStreams(int nQueries, int nStreams, Random random, HashMap<Integer, String> queriesHT,
			String workDir, String resultsDir, String plansDir, boolean singleCall) {
		int totalQueries = nQueries * nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this.recorder, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < nStreams; i++) {
			QueryStream stream = null;
			if( !this.multiple ) {
				stream = new QueryStream(i, this.resultsQueue, this.con, queriesHT, nQueries,
					workDir, resultsDir, plansDir, singleCall, random, this.recorder.system,
					this.savePlans, this.saveResults);
			}
			else {
				Connection con = this.createConnection(this.system, this.hostname, this.dbName);
				stream = new QueryStream(i, this.resultsQueue, con, queriesHT, nQueries,
						workDir, resultsDir, plansDir, singleCall, random, this.recorder.system,
						this.savePlans, this.saveResults);
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
			this.logger.error(e);
		}
	}

}
