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

	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public AnalyzeTables(String system, String hostname) {
		try {
			system = system.toLowerCase();
			String driverName = "";
			if( system.equals("hive") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10000/default", "hive", "");
			}
			else if( system.equals("presto") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8080/hive/default", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.equals("prestoemr") ) {
				Class.forName(prestoDriverName);
				con = DriverManager.getConnection("jdbc:presto://" + 
						hostname + ":8889/hive/default", "hive", "");
				((PrestoConnection)con).setSessionProperty("query_max_stage_count", "102");
			}
			else if( system.startsWith("spark") ) {
				Class.forName(hiveDriverName);
				con = DriverManager.getConnection("jdbc:hive2://" +
						hostname + ":10015/default", "", "");
			}
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
			this.recorder = new AnalyticsRecorder("analyze", system);
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

	/**
	 * @param args
	 * @throws SQLException
	 * 
	 * args[0] system to evaluate the queries (hive/presto)
	 * args[1] hostname of the server
	 * 
	 * all directories without slash
	 */
	public static void main(String[] args) throws SQLException {
		if( args.length < 2 ) {
			System.out.println("Incorrect number of arguments.");
			logger.error("Insufficient arguments.");
			System.exit(1);
		}
		AnalyzeTables prog = new AnalyzeTables(args[0], args[1]);
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

	private void executeAnalyzeTable(String table, int index) {
		QueryRecord queryRecord = null;
		try {
			queryRecord = new QueryRecord(index);
			System.out.println("\nAnalyzing table: " + table + "\n");
			this.logger.info("\nAnalyzing table: " + table + "\n");
			Statement stmt = con.createStatement();
			String sqlStr = "ANALYZE TABLE " + table + " COMPUTE STATISTICS";
			stmt.executeUpdate(sqlStr);
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

