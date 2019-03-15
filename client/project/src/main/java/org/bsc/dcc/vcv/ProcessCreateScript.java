package org.bsc.dcc.vcv;

import java.io.*;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessCreateScript {

	private static final String regExp = "(create table )([A-Za-z][A-Za-z0-9_]*)[^;]*(;)";

	/**
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory within the main work directory to store the generated .sql files
	 * args[2] .sql file to process containing the create table statements 
	 *         (it is assumed it is in the work directory).
	 */
	public static void main(String[] args) {
		ProcessCreateScript prog = new ProcessCreateScript();
		prog.processScript(args[0], args[1], args[2]);
	}

	public void processScript(String workDir, String subDir, String file) {
		String script = this.readFileContents(workDir + "/" + file);
		String regExp = "(create table )([A-Za-z][A-Za-z0-9_]*)[^;]*(;)";
		Pattern pattern = Pattern.compile(regExp);
		Matcher matcher = pattern.matcher(script);
		while (matcher.find()) {
		    String sqlStr = matcher.group(0);
		    String tableName = matcher.group(2);
		    generateCreateTableFile(workDir, subDir, tableName, sqlStr);
		}
	}
	
	public void generateCreateTableFile(String workDir, String subDir, String tableName, String sqlStr) {
		try {
			File temp = new File(workDir + "/" + subDir + "/" + tableName + ".sql");
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(workDir + "/" + subDir + "/" + tableName + ".sql");
		    PrintWriter printWriter = new PrintWriter(fileWriter);
		    printWriter.println(sqlStr);
		    printWriter.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	public String readFileContents(String filename) {
		BufferedReader inBR = null;
		String retVal = null;
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line = null;
			StringBuilder builder = new StringBuilder();
			while ((line = inBR.readLine()) != null) {
				builder.append(line + "\n");
			}
			retVal = builder.toString();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return retVal;
	}

}

