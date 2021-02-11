package org.bsc.dcc.vcv.etl;

import java.util.Arrays;
import java.util.Optional;

import org.bsc.dcc.vcv.Partitioning;


public class SQLWritePartitionedTest3 {
	

	public static String createTableStatement(String sqlQuery, String tableNameRoot, String tableName,
			String format, Optional<String> extTablePrefixCreated, boolean partition) {
		sqlQuery = sqlQuery.replace(tableNameRoot, tableName);
		StringBuilder builder = new StringBuilder(sqlQuery);
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		if( partition ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableNameRoot);
			if( pos != -1 )
				builder.append("PARTITIONED BY (" + Partitioning.partKeys[pos] + ") \n" );
		}
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		return builder.toString();
	}
	
	
	public static String insertStatement(String tableNameRoot, String tableName) {
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT INTO " + tableName + "\n");
		builder.append("SELECT * FROM " + tableNameRoot + "\n");
		return builder.toString();
	}
	
	
}


