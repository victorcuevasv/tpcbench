package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.*;

public class CreateDatabase {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;

	public CreateDatabase() {
		try {
			Class.forName(driverName);
			//con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
			con = DriverManager.getConnection("jdbc:hive2://hiveservercontainer:10000/default", "hive", "");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		CreateDatabase prog = new CreateDatabase();
		File directory = new File(args[0]);
		for (final File fileEntry : directory.listFiles()) {
	        if ( ! fileEntry.isDirectory() ) {
	            prog.createTable(fileEntry.getName());
	        }
	    }
	}

	private void createTable(String tableSQLfile) {
		try {
			String tableName = tableSQLfile.substring(0, tableSQLfile.indexOf('.'));
			System.out.println(tableName);
			Statement stmt = con.createStatement();
			/*
			stmt.execute("drop table if exists " + tableName);
			String sqlCreate = readFileContents(tableSQLfile);
			stmt.execute(sqlCreate);
			*/
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void loadTable(String tableName) {
		try {
			// NOTE: filepath has to be local to the hive server
			String filepath = "/tmp/a.txt";
			String sql = "load data local inpath '" + filepath + "' into table " + tableName;
			System.out.println("Running: " + sql);
			Statement stmt = con.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
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
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return retVal;
	}

}
