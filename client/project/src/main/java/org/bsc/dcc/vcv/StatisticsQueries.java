package org.bsc.dcc.vcv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StatisticsQueries {

	private static final Logger logger = LogManager.getLogger("AllLog");
	private final JarCreateTableReaderAsZipFile createTableReader;
	private final String workDir;
	private final String subDir;
	private final String outputDir;
	private final String jarFile;
	
	/**
	 * @param args
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory within the jar that contains the create table files
	 * args[2] output directory for queries
	 * args[3] jar file
	 * 
	 */
	// Open the connection (the server address depends on whether the program is
	// running locally or under docker-compose).
	public StatisticsQueries(String[] args) {
		this.workDir = args[0];
		this.subDir = args[1];
		this.outputDir = args[2];
		this.jarFile = args[3];
		this.createTableReader = new JarCreateTableReaderAsZipFile(this.jarFile, this.subDir);
	}

	
	public static void main(String[] args) {
		if( args.length != 4 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			logger.error("Incorrect number of arguments: " + args.length);
			System.exit(1);
		}
		StatisticsQueries prog = new StatisticsQueries(args);
		prog.createQueries();
	}
	
	
	private void createQueries() {
		// Process each .sql create table file found in the directory.
		List<String> unorderedList = this.createTableReader.getFiles();
		List<String> orderedList = unorderedList.stream().sorted().collect(Collectors.toList());
		for (final String fileName : orderedList) {
			String sqlCreate = this.createTableReader.getFile(fileName);
			this.processCreateTableStmt(fileName, sqlCreate);
		}
	}
	
	
	private void processCreateTableStmt(String sqlCreateFilename, String sqlCreate) {
		String tableName = sqlCreateFilename.substring(0, sqlCreateFilename.indexOf('.'));
		System.out.println("Processing table: " + tableName);
		this.logger.info("Processing table: " + tableName);
		// Skip the dbgen_version table.
		if (tableName.equals("dbgen_version")) {
			System.out.println("Skipping: " + tableName);
			return;
		}
		List<String> columnNamesList = this.extractColumnNames(sqlCreate);
		String sqlStr = this.createDistinctValuesStmt(tableName, columnNamesList);
		this.saveSQLFile(tableName, sqlStr, this.outputDir, "distinct");
	}
		
	
	private String createDistinctValuesStmt(String tableName, List<String> columns) {
		StringBuilder builder = new StringBuilder();
		builder.append("with table_stats as( \n");
		for(String column : columns) {
			builder.append("(select '" + column + "' as col_name, count(distinct(" + 
					column + ")) as dist_value_count from " + 
					tableName + ") union \n");
		}
		builder.append("select * from table_stats \n");
		builder.append("order by col_name \n");
		return builder.toString();
	}
	
	
	private List<String> extractColumnNames(String sqlStr) {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
				continue;
				if( line.trim().startsWith("create") || line.trim().startsWith("(") || 
						line.trim().startsWith(")") || line.trim().startsWith("primary key") )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				list.add(tokenizer.nextToken());
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
		return list;
	}

	
	public void saveSQLFile(String tableName, String sqlStr, String outputDir, String outputSubDir) {
		try {
			String sqlFileName = this.workDir + "/" + this.outputDir + "/" + outputSubDir +
								"/" + tableName + ".sql";
			File temp = new File(sqlFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(sqlFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(sqlStr);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}
	

}


