package org.bsc.dcc.vcv.etl;

import java.util.Optional;


public class SQLWriteUnPartitionedTest2 {
	

	public static String createTableStatement(String sqlQuery, String tableNameRoot, String tableName, 
			String format, Optional<String> extTablePrefixCreated) {
		sqlQuery = org.bsc.dcc.vcv.etl.Util.incompleteCreateTable(sqlQuery);
		sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
		StringBuilder builder = new StringBuilder();
		builder.append(sqlQuery + "\n");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		return builder.toString();
	}

	
	public static String insertStatement(String sqlQuery, String tableNameRoot, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		builder.append("SELECT * FROM " + tableNameRoot + "\n");
		return builder.toString();
	}
	
	
}


