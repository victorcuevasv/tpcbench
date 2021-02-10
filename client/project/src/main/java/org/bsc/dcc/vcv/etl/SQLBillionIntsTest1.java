package org.bsc.dcc.vcv.etl;

import java.util.Optional;

public class SQLBillionIntsTest1 {
	

	public static String createTableStatement(String sqlQuery, String tableName,
			String format, Optional<String> extTablePrefixCreated) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + tableName + " ");
		builder.append("(" + tableName + " int) ");
		builder.append("USING " + format);
		if( format.equals("parquet") )
			builder.append("\nOPTIONS ('compression'='snappy')");
		builder.append("\nLOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	
	public static String insertStatement(String sqlQuery, String tableNameRoot, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		builder.append("SELECT " + tableName + "\n");
		builder.append("FROM " + tableNameRoot + "\n");
		return builder.toString();
	}
	
	
}


