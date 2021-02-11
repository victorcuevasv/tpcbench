package org.bsc.dcc.vcv.etl;

import java.util.Arrays;
import java.util.Optional;
import org.bsc.dcc.vcv.Partitioning;

public class SQLThousandColsTest6 {
	
	
	public static String createTableStatement(String sqlQuery, String tableNameRoot,
			String tableName, String format, Optional<String> extTablePrefixCreated, boolean partition) {
		StringBuilder builder = new StringBuilder("CREATE TABLE " + tableName + " ");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		if( partition ) {
			int pos = Arrays.asList(Partitioning.tables).indexOf(tableNameRoot);
			if( pos != -1 )
				builder.append("partitioned BY (d_date_sk) AS\n" );
		}
		builder.append(sqlQuery);
		return builder.toString();
	}
	

}


