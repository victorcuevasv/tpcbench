package org.bsc.dcc.vcv.etl;

import java.util.Optional;

import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;


public class SQLWriteUnPartitionedTest2 {
	

	public static String createTableStatementSpark(String sqlQuery, String tableNameRoot, String tableName, 
			String format, Optional<String> extTablePrefixCreated, boolean partition) {
		sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
		sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
		return CreateDatabaseSparkUtil.internalCreateTable(sqlQuery, tableNameRoot, 
				extTablePrefixCreated, format, partition);
	}

	
	public static String insertStatement(String tableNameRoot, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		builder.append("SELECT * FROM " + tableNameRoot + "\n");
		return builder.toString();
	}
	
	
}


