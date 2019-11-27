package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.sql.DriverManager;
import java.io.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.StringTokenizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SnowflakeHistory {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private static final String snowflakeDriverName = "net.snowflake.client.jdbc.SnowflakeDriver";
	private Connection con;
	private final String dbName;
	private final String hostname;
	
	
	/**
	 * @param args
	 * 
	 * args[0] schema (database) name
	 * args[1] host name
	 * args[2] session ID
	 * args[3] output file name
	 * 
	 */
	public SnowflakeHistory(String[] args) {
		this.dbName = args[0];
		this.hostname = args[1];
		try {
			String driverName = "";
			Class.forName(snowflakeDriverName);
			this.con = DriverManager.getConnection("jdbc:snowflake://" + this.hostname + "/?" +
						"user=bsctest" + "&password=c4[*4XYM1GIw" + "&db=" + this.dbName +
						"&schema=" + this.dbName + "&warehouse=testwh");
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (SQLException e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in ExecuteQueries constructor.");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	

	public static void main(String[] args) throws SQLException {
		if( args.length != 4 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Insufficient arguments: " + args.length);
			System.exit(1);
		}
		SnowflakeHistory prog = new SnowflakeHistory(args);
		prog.saveSnowflakeHistory(args[2], args[3]);
		prog.closeConnection();
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
	
	
	private void saveSnowflakeHistory(String sessionID, String outFileName) {
		try {
			String columnsStr = this.createSnowflakeHistoryFileAndColumnList(outFileName);
			Statement historyStmt = this.con.createStatement();
			String historySQL = "select " + columnsStr + " " + 
			"from table( " + 
			"information_schema.query_history_by_session(" + sessionID + ")) " +
			"where query_type='SELECT' " +
			"order by start_time;";
			ResultSet rs = historyStmt.executeQuery(historySQL);
			this.saveResults(outFileName, rs, true);
			historyStmt.close();
		}
		catch(Exception e) {
			e.printStackTrace();
			this.logger.error("Error in saveSnowflakeHistory");
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

	
	private void closeConnection() {
		try {
			this.con.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
			this.logger.error(e);
		}
	}

}
