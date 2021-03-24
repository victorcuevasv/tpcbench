package org.bsc.dcc.vcv.etl;

import java.util.List;
import java.util.Optional;
import org.bsc.dcc.vcv.CreateDatabaseSparkUtil;


public class SQLWritePartitionedTest3 {
	

	public static String createTableStatementSpark(String sqlQuery, String tableNameRoot, String tableName,
			String format, Optional<String> extTablePrefixCreated, boolean partition) {
		sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
		return CreateDatabaseSparkUtil.internalCreateTable(sqlQuery, tableNameRoot, 
				extTablePrefixCreated, format, partition);
	}
	
	
	public static String insertStatement(String sqlQuery, String tableNameRoot, String tableName,
			String format) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		List<String> columns = CreateDatabaseSparkUtil.extractColumnNames(sqlQuery);
		String selectStmt = CreateDatabaseSparkUtil.createPartitionSelectStmt(tableNameRoot, columns, "",
				format, false);
		builder.append(selectStmt);
		return builder.toString();
	}
	
	
}


