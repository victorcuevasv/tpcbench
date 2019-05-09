package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
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

public class ExecuteQueriesConcurrentPrestoCLI implements ConcurrentExecutor {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorderConcurrent recorder;
	private ExecutorService executor;
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 100;
	private long seed;
	private Random random;
	private String system;
	private String hostnameAndPort;
	boolean savePlans;
	boolean saveResults;
	private String dbName;

	public ExecuteQueriesConcurrentPrestoCLI(String system, String hostnameAndPort,
			boolean savePlans, boolean saveResults, String dbName) {
		this.savePlans = savePlans;
		this.saveResults = saveResults;
		this.system = system;
		this.hostnameAndPort = hostnameAndPort;
		this.dbName = dbName;
		this.recorder = new AnalyticsRecorderConcurrent("tput", this.system);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
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
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) {
		if( args.length != 12 ) {
			System.out.println("Insufficient arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		boolean multipleUnused = Boolean.parseBoolean(args[8]);
		boolean savePlans = Boolean.parseBoolean(args[9]);
		boolean saveResults = Boolean.parseBoolean(args[10]);
		ExecuteQueriesConcurrentPrestoCLI prog = new ExecuteQueriesConcurrentPrestoCLI(args[4], args[5],
				savePlans, saveResults, args[11]);
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
			QueryStreamPrestoCLI stream = new QueryStreamPrestoCLI(i, this.resultsQueue, queriesHT, nQueries,
						workDir, resultsDir, plansDir, singleCall, random, this.recorder.system,
						this.savePlans, this.saveResults, this.hostnameAndPort, this.dbName);
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
		//do nothing
		;
	}

}


