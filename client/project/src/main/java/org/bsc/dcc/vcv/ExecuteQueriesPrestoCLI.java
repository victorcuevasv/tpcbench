package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.List;
import java.io.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.StringTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecuteQueriesPrestoCLI {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorder recorder;
	boolean savePlans;
	boolean saveResults;
	String hostnameAndPort;

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public ExecuteQueriesPrestoCLI(String system, String hostnameAndPort, boolean savePlans, boolean saveResults) {
		try {
			this.savePlans = savePlans;
			this.saveResults = saveResults;
			system = system.toLowerCase();
			this.recorder = new AnalyticsRecorder("power", system);
			this.hostnameAndPort = hostnameAndPort;
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private String getPrestoDefaultSessionOpts() {
		return "SET SESSION query_max_stage_count = 102;\n" + 
		"SET SESSION join_reordering_strategy = 'AUTOMATIC';\n" + 
		"SET SESSION join_distribution_type = 'AUTOMATIC';\n" + 
		"SET SESSION task_concurrency = 8;\n";
	}

	/**
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory that contains the queries
	 * args[2] subdirectory of work directory to store the results
	 * args[3] subdirectory of work directory to store the execution plans
	 * args[4] system to evaluate the queries (hive/presto)
	 * args[5] hostname and port of the server (e.g. namenodecontainer:8080)
	 * args[6] save plans (boolean)
	 * args[7] save results (boolean)
	 * args[8] OPTIONAL: query file
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws Exception {
		if( args.length < 8 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		boolean savePlans = Boolean.parseBoolean(args[6]);
		boolean saveResults = Boolean.parseBoolean(args[7]);
		ExecuteQueriesPrestoCLI prog = new ExecuteQueriesPrestoCLI(args[4], args[5], savePlans, saveResults);
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
		prog.recorder.header();
		String queryFile = args.length >= 9 ? args[8] : null;
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				if( queryFile != null ) {
					if( ! fileEntry.getName().equals(queryFile) )
						continue;
				}
				prog.executeQueryFile(args[0], fileEntry, args[2], args[3], false);
			}
		}
	}

	// Execute the query (or queries) from the provided file.
	private void executeQueryFile(String workDir, File sqlFile, String resultsDir,
			String plansDir, boolean singleCall) {
		QueryRecord queryRecord = null;
		try {
			String fileName = sqlFile.getName().substring(0, sqlFile.getName().indexOf('.'));
			String nQueryStr = fileName.replaceAll("[^\\d.]", "");
			int nQuery = Integer.parseInt(nQueryStr);
			queryRecord = new QueryRecord(nQuery);
			String sqlStr = readFileContents(sqlFile.getAbsolutePath());
			this.logger.info("\nExecuting query: " + sqlFile.getName() + "\n");
			this.executeQuerySingleCall(workDir, resultsDir, plansDir, fileName, sqlStr, queryRecord);
			//Record the results file size.
			File resultsFile = new File(workDir + "/" + resultsDir + "/" + "power" + "/" + 
					this.recorder.system + "/" + fileName + ".txt");
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
			this.recorder.record(queryRecord);
		}
	}
	
	// Execute the queries from the provided file.
	private void executeQuerySingleCall(String workDir, String resultsDir, String plansDir,
			String fileName, String sqlStrFull, QueryRecord queryRecord) throws Exception {
		//Obtain the query plan.	
		this.executeQuery("EXPLAIN " + sqlStrFull, this.savePlans, 
			workDir + "/" + plansDir + "/" + "power" + "/" + this.recorder.system + "/" + fileName + ".txt");
		// Execute the query.
		queryRecord.setStartTime(System.currentTimeMillis());
		System.out.println("Executing query " + fileName + ".");
		this.executeQuery(this.getPrestoDefaultSessionOpts() + sqlStrFull, this.saveResults,
			workDir + "/" + resultsDir + "/" + "power" + "/" + this.recorder.system + "/" + fileName + ".txt");
	}
	
	private void executeQuery(String sql, boolean save, String outFile) 
		throws Exception {
		int retVal = 0;
		try {
			ProcessBuilder pb = new ProcessBuilder();
			String cmd = "export PRESTO_PAGER= ; " + 
			"/opt/presto --server " + this.hostnameAndPort + 
			" --catalog hive --schema default --execute \"" + sql +"\"";
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

}
