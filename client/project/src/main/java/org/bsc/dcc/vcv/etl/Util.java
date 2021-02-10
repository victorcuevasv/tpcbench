package org.bsc.dcc.vcv.etl;

public class Util {

	
	public static String incompleteCreateTable(String sqlCreate) {
		boolean hasPrimaryKey = sqlCreate.contains("primary key");
		// Remove the 'not null' statements.
		sqlCreate = sqlCreate.replace("not null", "");
		// Split the SQL create table statement in lines.
		String lines[] = sqlCreate.split("\\r?\\n");
		// Add all of the lines in the original SQL to the new statement, except those
		// which contain the primary key statement (if any) and the closing parenthesis.
		// For the last column statement, remove the final comma.
		StringBuilder builder = new StringBuilder();
		int tail = hasPrimaryKey ? 3 : 2;
		for (int i = 0; i < lines.length - tail; i++)
			builder.append(lines[i] + "\n");
		// Change the last comma for a space (since the primary key statement was
		// removed).
		char[] commaLineArray = lines[lines.length - tail].toCharArray();
		commaLineArray[commaLineArray.length - 1] = ' ';
		builder.append(new String(commaLineArray) + "\n");
		// Close the parenthesis.
		builder.append(") \n");
		// Version 2.1 of Hive does not recognize integer, so use int instead.
		return builder.toString().replace("integer", "int");
	}
	
	
}


