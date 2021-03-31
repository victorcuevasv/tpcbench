package org.bsc.dcc.vcv.etl;

import java.util.List;
import java.util.Optional;

import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;


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
		String sqlSelect = CreateDatabaseSparkUtil.createPartitionSelectStmt(tableNameRoot, columns,
				"", format, false);
		builder.append(sqlSelect);
		return builder.toString();
	}
	
	
}


