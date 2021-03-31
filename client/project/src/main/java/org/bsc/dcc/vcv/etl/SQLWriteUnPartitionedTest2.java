package org.bsc.dcc.vcv.etl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;
import org.bsc.dcc.vcv.Partitioning;


public class SQLWriteUnPartitionedTest2 {
	

	public static String createTableStatementSpark(String sqlQuery, String tableNameRoot, String tableName, 
			String format, Optional<String> extTablePrefixCreated, boolean partition) {
		sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
		sqlQuery = CreateDatabaseSparkUtil.internalCreateTable(sqlQuery, tableNameRoot, 
				extTablePrefixCreated, format, partition);
		return sqlQuery.replace(tableNameRoot, tableName);
	}

	
	//It is assumed that the tableNameRoot table is partitioned and that the null values for the partition
	//attribute have been removed.
	public static String insertStatement(String sqlQuery, String tableNameRoot, String tableName,
			String suffix, String format, boolean partitionIgnoreNulls) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
		List<String> columns = CreateDatabaseSparkUtil.extractColumnNames(sqlQuery);
		String sqlSelect = null;
		if( format.equalsIgnoreCase("parquet") )
			sqlSelect = SQLWriteUnPartitionedTest2.createParquetSelectStmt(tableNameRoot, columns, "", 
					partitionIgnoreNulls);
		else
			sqlSelect = CreateDatabaseSparkUtil.createPartitionSelectStmt(tableNameRoot, columns,
				"", format, false);
		builder.append(sqlSelect);
		return builder.toString();
	}
	
	public static String createParquetSelectStmt(String tableName, List<String> columns, String suffix,
			boolean partitionIgnoreNulls) {
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT \n");
		//For the unpartitioned table the partition attribute does not need to be stated last.
		String atts = columns
				.stream()
				.collect(Collectors.joining(",\n"));
		builder.append(atts + "\n");
		builder.append("FROM " + tableName + suffix + "\n");
		if( partitionIgnoreNulls ) {
			builder.append("WHERE " + 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] +
					" is not null \n");
		}
		builder.append("DISTRIBUTE BY " + 
					Partitioning.partKeys[Arrays.asList(Partitioning.tables).indexOf(tableName)] + "\n");
		return builder.toString();
	}
	
	
}


