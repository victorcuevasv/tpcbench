package org.bsc.dcc.vcv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryStreamPrestoCLI implements Callable<Void> {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private int nStream;
	private HashMap<Integer, String> queriesHT;
	private int nQueries;
	private String workDir;
	private String resultsDir;
	private String plansDir;
	private boolean singleCall;
	private Random random;
	private final String system;
	boolean savePlans;
	boolean saveResults;
	private String hostnameAndPort;

	public QueryStreamPrestoCLI(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			HashMap<Integer, String> queriesHT, int nQueries,
			String workDir, String resultsDir, String plansDir, boolean singleCall, 
			Random random, String system, boolean savePlans, boolean saveResults,
			String hostnameAndPort) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.queriesHT = queriesHT;
		this.nQueries = nQueries;
		this.workDir = workDir;
		this.resultsDir = resultsDir;
		this.plansDir = plansDir;
		this.singleCall = singleCall;
		this.random = random;
		this.system = system;
		this.savePlans = savePlans;
		this.saveResults = saveResults;
		this.hostnameAndPort = hostnameAndPort;
	}
	
	private String getPrestoDefaultSessionOpts() {
		return "SET SESSION query_max_stage_count = 102;\n" + 
		"SET SESSION join_reordering_strategy = 'AUTOMATIC';\n" + 
		"SET SESSION join_distribution_type = 'AUTOMATIC';\n" + 
		"SET SESSION task_concurrency = 8;\n";
	}

	@Override
	public Void call() {
		//Integer[] queries = this.queriesHT.keySet().toArray(new Integer[] {});
		//Arrays.sort(queries);
		//this.shuffle(queries);
		int[] queries = StreamsTable.matrix[this.nStream];
		for(int i = 0; i < queries.length; i++) {
			String sqlStr = this.queriesHT.get(queries[i]);
			this.executeQuery(this.nStream, this.workDir, queries[i], sqlStr,
					this.resultsDir, this.plansDir, this.singleCall, i);
		}
		return null;
	}

	// Execute the query (or queries) from the provided file.
	private void executeQuery(int nStream, String workDir, int nQuery, String sqlStr, String resultsDir,
			String plansDir, boolean singleCall, int item) {
		QueryRecordConcurrent queryRecord = null;
		String fileName = "query" + nQuery;
		try {
			queryRecord = new QueryRecordConcurrent(nStream, nQuery);
			// Execute the query or queries.
			this.executeQuerySingleCall(nStream, workDir, resultsDir, plansDir,
						fileName, sqlStr, queryRecord, item);
			// Record the results file size.
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + "tput" + "/" + 
					this.system + "/" + nStream + "_" + fileName + ".txt");
			queryRecord.setResultsSize(resultsFile.length());
			queryRecord.setSuccessful(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		finally {
			queryRecord.setEndTime(System.currentTimeMillis());
			this.resultsQueue.add(queryRecord);
		}
	}

	// Execute the queries from the provided file.
	private void executeQuerySingleCall(int nStream, String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStrFull, QueryRecordConcurrent queryRecord, int item) 
			throws Exception {
		//Obtain the query plan.	
		this.executeQuery("EXPLAIN " + sqlStrFull, this.savePlans, 
			workDir + "/" + plansDir + "/" + "tput" + "/" + this.system + "/" + 
			nStream + "_" + item + "_" + fileName + ".txt");
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		System.out.println("Executing query " + fileName + ".");
		this.executeQuery(this.getPrestoDefaultSessionOpts() + sqlStrFull, this.saveResults,
			workDir + "/" + resultsDir + "/" + "tput" + "/" + this.system + "/" + 
			nStream + "_" + item + "_" + fileName + ".txt");
	}

	private void executeQuery(String sql, boolean save, String outFile) throws Exception {
		int retVal = 0;
		try {
				ProcessBuilder pb = new ProcessBuilder();
			// "${QUERY//\"/\\\"}" Replace in the QUERY variable " with \" in bash
			CharSequence quote = "\"";
			CharSequence replacement = "\\\"";
			String cmd = "export PRESTO_PAGER= ; " + 
			"/opt/presto --server " + this.hostnameAndPort + 
			" --catalog hive --schema  default --execute \"" + sql.replace(quote, replacement) + "\"";
			pb.command("bash", "-c", cmd);
			// pb.environment().put("FOO", "BAR");
			// From the DOC: Initially, this property is false, meaning that the
			// standard output and error output of a subprocess are sent to two
			// separate streams
			pb.redirectErrorStream(true);
			Process pr = pb.start();
			BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line;
			File tmp = new File(outFile);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(outFile);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			while ((line = in.readLine()) != null) {
				//System.out.println(line);
				if( save )
					printWriter.println(line);
			}
			retVal = pr.waitFor();
			in.close();
			printWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if( retVal != 0 )
				throw new RuntimeException("ERROR: presto-cli returned a value other than 0.");
		}
	}
	
	private void shuffle(Integer[] array) {
		for(int i = 0; i < array.length; i++) {
			int randPos = this.random.nextInt(array.length);
			int temp = array[i];
			array[i] = array[randPos];
			array[randPos] = temp;
		}
	}

}

