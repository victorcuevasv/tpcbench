package org.bsc.dcc.vcv;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessCreateScript {

	private static final String regExp = "(create table )([A-Za-z][A-Za-z0-9_]*)[^;]*(;)";

	public static void main(String[] args) {
		ProcessCreateScript prog = new ProcessCreateScript();
		prog.processScript(args[0], args[1]);
	}

	public void processScript(String workDir, String file) {
		String script = this.readFileContents(workDir + "/" + file);
		String regExp = "(create table )([A-Za-z][A-Za-z0-9_]*)[^;]*(;)";
		Pattern pattern = Pattern.compile(regExp);
		Matcher matcher = pattern.matcher(script);
		while (matcher.find()) {
		    String sqlStr = matcher.group(0);
		    String tableName = matcher.group(2);
		    generateCreateTableFile(workDir, tableName, sqlStr);
		}
	}
	
	public void generateCreateTableFile(String workDir, String tableName, String sqlStr) {
		try {
			FileWriter fileWriter = new FileWriter(workDir + "/tables" + "/" + tableName + ".sql");
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

