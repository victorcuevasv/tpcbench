package org.bsc.dcc.vcv.etl;

import java.util.Optional;


public class SQLDeepCopyTest5 {

	
	public static String createTableStatement(String sqlQuery, String tableNameRoot, String tableName,
			String format, Optional<String> extTablePrefixCreated) {
		StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE " + tableName + "\n");
		builder.append("USING " + format + "\n");
		if( format.equals("parquet") )
			builder.append("OPTIONS ('compression'='snappy')\n");
		builder.append("LOCATION '" + extTablePrefixCreated.get() + "/" + tableName + "' \n");
		builder.append("AS\n");
		builder.append("SELECT * FROM " + tableNameRoot + "_denorm");
		return builder.toString();
	}
	

}


