package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.facebook.presto.jdbc.PrestoConnection;

public class AnalyzeTables {

	private static final String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
	private static final String prestoDriverName = "com.facebook.presto.jdbc.PrestoDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger("AllLog");
	private AnalyticsRecorder recorder;
	private String workDir;
	private String dbName;
	private String folderName;
	private String experimentName;
	private String system;
	private String test;
	private int instance;
	private boolean computeForCols;
	private String hostname;
	

	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] schema (database) name
	 * args[2] results folder name (e.g. for Google Drive)
	 * args[3] experiment name (name of subfolder within the results folder)
	 * args[4] system name (system name used within the logs)
	 * args[5] test name (i.e. load)
	 * args[6] experiment instance number
	 * args[7] compute statistics for columns (true/false)
	 * args[8] hostname of the server
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public AnalyzeTables(String[] args) {
		this.workDir = args[0];
		this.dbName = args[1];
		this.folderName = args[2];
		this.experimentName = args[3];
		this.system = args[4];
		this.test = args[5];
		this.instance = Integer.parseInt(args[6]);
		this.computeForCols = Boolean.parseBoolean(args[7]);
		this.hostname = args[8];
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
				con = DriverManager.getConnection("jdbc:presto://" + 
						this.hostname + ":8889/hive/" + this.dbName, "hive", "");
				((PrestoConnection)this.con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/" + dbName, "hive", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
			this.recorder = new AnalyticsRecorder(this.workDir, this.folderName, this.experimentName,
					this.system, this.test, this.instance);
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


	public static void main(String[] args) throws SQLException {
		if( args.length != 9 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		AnalyzeTables prog = new AnalyzeTables(args);
		prog.configureMapreduce();
		String[] tables = {"call_center", "catalog_page", "catalog_returns", "catalog_sales",
							"customer", "customer_address", "customer_demographics", "date_dim",
							"household_demographics", "income_band", "inventory", "item",
							"promotion", "reason", "ship_mode", "store", "store_returns",
							"store_sales", "time_dim", "warehouse", "web_page", "web_returns",
							"web_sales", "web_site"};
		for(int i = 0; i < tables.length; i++) {
			prog.executeAnalyzeTable(tables[i], i);
		}
		prog.closeConnection();
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
			String sqlStr = "ANALYZE TABLE " + table + " COMPUTE STATISTICS";
			stmt.executeUpdate(sqlStr);
			if( this.computeForCols ) {
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


