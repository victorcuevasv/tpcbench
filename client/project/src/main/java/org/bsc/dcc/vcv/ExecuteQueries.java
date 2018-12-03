package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.sql.DriverManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecuteQueries {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private Connection con;
	private static final Logger logger = LogManager.getLogger(ExecuteQueries.class);

	// Open the connection (the server address depends on whether the program is
	// running locally or
	// under docker-compose).
	public ExecuteQueries() {
		try {
			Class.forName(driverName);
			// con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
			// "hive", "");
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
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory of work directory that contains the queries
	 * args[2] subdirectory of work directory to store the results
	 * 
	 * all of them without slash
	 */
	public static void main(String[] args) throws SQLException {
		ExecuteQueries prog = new ExecuteQueries();
		File directory = new File(args[0] + "/" + args[1]);
		// Process each .sql file found in the directory.
		File[] files = directory.listFiles();
		Arrays.sort(files);
		for (final File fileEntry : files) {
			if (!fileEntry.isDirectory()) {
				prog.executeQuery(args[0], fileEntry, args[2]);
				break;
			}
		}
	}

	// Execute a query from the provided file.
	private void executeQuery(String workDir, File sqlFile, String resultsDir) {
		try {
			System.out.println("Processing: " + sqlFile.getName());
			logger.info("Processing: " + sqlFile.getName());
			String fileName = sqlFile.getName().substring(0, sqlFile.getName().indexOf('.'));
			String sqlQuery = readFileContents(sqlFile.getAbsolutePath());
			//Remove the last semicolon.
			sqlQuery = sqlQuery.trim();
			sqlQuery = sqlQuery.substring(0, sqlQuery.length() - 1);
			FileWriter fileWriter = new FileWriter(workDir + "/" + resultsDir + "/" + fileName + ".txt");
			PrintWriter printWriter = new PrintWriter(fileWriter);
			Statement stmt = con.createStatement();
			ResultSet res = stmt.executeQuery(sqlQuery);
			ResultSetMetaData metadata = res.getMetaData();
		    int nCols = metadata.getColumnCount();
			while (res.next()) {
				StringBuilder rowBuilder = new StringBuilder();
		        for (int i = 1; i <= nCols; i++) {
		            rowBuilder.append(res.getString(i) + ", ");          
		        }
				printWriter.println(rowBuilder.toString());
			}
			printWriter.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
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


