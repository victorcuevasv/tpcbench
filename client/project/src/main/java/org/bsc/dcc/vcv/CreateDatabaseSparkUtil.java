package org.bsc.dcc.vcv;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Optional;
import java.io.*;


public class CreateDatabaseSparkUtil {
	
	// Based on the supplied incomplete SQL create statement, generate a full create
	// table statement for an internal parquet table in Hive.
	public static String internalCreateTable(String incompleteSqlCreate, String tableName,
			Optional<String> extTablePrefixCreated, String format, boolean partition) {
		StringBuilder builder = new StringBuilder(incompleteSqlCreate);
		// Add the stored as statement.
		if( extTablePrefixCreated.isPresent() ) {
			if( format.equalsIgnoreCase("DELTA") )
				builder.append("USING DELTA \n");
			else if( format.equalsIgnoreCase("PARQUET") )
				//builder.append("USING org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat \n");
				builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			else if( format.equalsIgnoreCase("ICEBERG") )
				builder.append("USING ICEBERG \n");
			if( partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n" );
			}
			builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		}
		else {
			builder.append("USING PARQUET \n" + "OPTIONS ('compression'='snappy') \n");
			if( partition ) {
				int pos = Arrays.asList(Partitioning.tables).indexOf(tableName);
				if( pos != -1 )
					//Use for Hive format.
					//builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + " integer) \n" );
					builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n");
			}
			//Use for Hive format.
			//builder.append("STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\") \n");
		}
		return builder.toString();
	}
	
	
	public static List<String> extractColumnNames(String sqlStr) {
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new StringReader(sqlStr));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
				continue;
				if( line.trim().startsWith("create") || line.trim().startsWith("(") || line.trim().startsWith(")") )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				list.add(tokenizer.nextToken());
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return list;
	}
	
	
	public static String createPartitionSelectStmt(String tableName, List<String> columns, String suffix,
			String format, boolean partitionIgnoreNulls) {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT \n");
		if( format.equalsIgnoreCase("parquet") ) {
			for(String column : columns) {
				if ( column.equalsIgnoreCase(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] ))
					continue;
				else
					builder.append(column + ", \n");
			}
			builder.append(Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + " \n");
		}
		else
			builder.append("* \n");
		builder.append("FROM " + tableName + suffix + "\n");
		if( partitionIgnoreNulls ) {
			builder.append("WHERE " + 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] +
					" is not null \n");
		}
		if( format.equals("iceberg") ) {
			builder.append("ORDER BY " + 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + "\n");
		}
		return builder.toString();
	}
	
	
	public static String extractPrimaryKey(String sqlCreate) {
		String primaryKeyLine = Stream.of(sqlCreate.split("\\r?\\n")).
				filter(s -> s.contains("primary key")).findAny().orElse(null);
		if( primaryKeyLine == null ) {
			System.out.println("Null value in extractPrimaryKey.");
			this.logger.error("Null value in extractPrimaryKey.");
		}
		String primaryKey = primaryKeyLine.trim();
		primaryKey = primaryKey.substring(primaryKey.indexOf('(') + 1, primaryKey.indexOf(')'));
		return primaryKey.replace(" ", "");
	}
	

}


