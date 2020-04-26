package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
	private static final String databricksDriverName = "com.simba.spark.jdbc41.Driver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final AnalyticsRecorder recorder;
	private final String workDir;
	private final String dbName;
	private final String resultsDir;
	private final String experimentName;
	private final String system;
	private final String test;
	private final int instance;
	private final boolean computeForCols;
	private final String hostname;
	
	
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
	 * args[5] test name (i.e. load)
	 * args[6] experiment instance number
	 * args[7] compute statistics for columns (true/false)
	 * args[8] hostname of the server
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public AnalyzeTables(String[] args) {
		if( args.length != 9 ) {
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
				((PrestoConnection)this.con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( this.system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				this.con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hive", "");
				((PrestoConnection)this.con).setSessionProperty("query_max_stage_count", "102");
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
				this.con = DriverManager.getConnection("jdbc:hive2://" +
						this.hostname + ":10015/" + this.dbName, "hive", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
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
		if( this.system.equals("hive") )
			this.configureMapreduce();
		String[] tables = {"call_center", "catalog_page", "catalog_returns", "catalog_sales",
							"customer", "customer_address", "customer_demographics", "date_dim",
							"household_demographics", "income_band", "inventory", "item",
							"promotion", "reason", "ship_mode", "store", "store_returns",
							"store_sales", "time_dim", "warehouse", "web_page", "web_returns",
							"web_sales", "web_site"};
		this.recorder.header();
		for(int i = 0; i < tables.length; i++) {
			if( this.system.startsWith("spark") )
				this.executeAnalyzeTableSpark(tables[i], i);
			else
				this.executeAnalyzeTable(tables[i], i);
		}
		this.recorder.close();
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
	
	
	private void executeAnalyzeTable(String table, int index) {
		QueryRecord queryRecord = null;
		try {
			queryRecord = new QueryRecord(index);
			System.out.println("\nAnalyzing table: " + table + "\n");
			this.logger.info("\nAnalyzing table: " + table + "\n");
			Statement stmt = con.createStatement();
			String sqlStr = null;
			if( this.system.equals("hive") )
				sqlStr = "ANALYZE TABLE " + table + " COMPUTE STATISTICS";
			else if( this.system.startsWith("presto") )
				sqlStr = "ANALYZE " + table;
			queryRecord.setStartTime(System.currentTimeMillis());
			stmt.executeUpdate(sqlStr);
			if( this.system.equals("hive") && this.computeForCols ) {
				String sqlStrCols = "ANALYZE TABLE " + table + " COMPUTE STATISTICS FOR COLUMNS";
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
	
	
	private void executeAnalyzeTableSpark(String tableName, int index) {
		QueryRecord queryRecord = null;
		try {
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
				ResultSet rs = stmt.executeQuery("DESCRIBE " + tableName);
				String columnsStr = extractColumns(rs);
				stmt.executeUpdate("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS FOR COLUMNS " + 
						columnsStr);
			}
			else
				stmt.executeUpdate("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS");
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
	
	
	private String extractColumns(ResultSet rs) 
			throws SQLException {
		StringBuilder builder = new StringBuilder();
		boolean first = true;
		while (rs.next()) {
			if( ! first )
				builder.append(", ");
			builder.append(rs.getString(0));
			first = false;
		}
		rs.close();
		return builder.toString();
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


