package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

public class ProcessParallelDataFiles {

	/**
	 * @param args
	 * 
	 * args[0] directory that contains the .dat files
	 * args[1] name of the output script
	 */
	public static void main(String[] args) {
		if( args.length < 2 ) {
			System.out.println("Usage: ProcessParallelDataFiles <files dir> <output script name>.");
			System.exit(1);
		}
		ProcessParallelDataFiles prog = new ProcessParallelDataFiles();
		Map<String, List<String>> tablesHT = prog.processFiles(args[0]);
		prog.printTablesHT(tablesHT);
		String scriptStr = prog.generateScript(tablesHT);
		prog.saveScript(scriptStr, args[0], args[1]);
	}
	
	private Map<String, List<String>> processFiles(String filesDir) {
		File directory = new File(filesDir);
		File[] files = directory.listFiles();
		Map<String, List<String>> tablesHT = new HashMap<String, List<String>>();
		for (final File file : files) {
			//It is not to be expected to find directories, but check.
			if ( ! file.isDirectory() ) {
				String tableName = getTableName(file.getName());
				List<String> tableFiles = tablesHT.get(tableName);
				if( tableFiles == null ) {
					tableFiles = new ArrayList<String>();
					tablesHT.put(tableName, tableFiles);
				}
				tableFiles.add(file.getName());
			}
		}
		return tablesHT;
	}
	
	private void printTablesHT(Map<String, List<String>> tablesHT) {
		Set<String> keySet = tablesHT.keySet();
		for(String table : keySet) {
			List<String> files = tablesHT.get(table);
			System.out.println(table + ":");
			for(String file : files)
				System.out.println("\t" + file);
		}
	}
	
	private String generateScript(Map<String, List<String>> tablesHT) {
		StringBuilder sb = new StringBuilder();
		sb.append("#!/bin/bash" + "\n");
		sb.append("DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" >/dev/null && pwd )\"\n");
		Set<String> keySet = tablesHT.keySet();
		for(String table : keySet) {
			List<String> files = tablesHT.get(table);
			sb.append("mkdir $DIR/" + table + "\n");
			for(String file : files)
				sb.append("mv" + " " + "$DIR/" + file + " " + "$DIR/" + table + "\n");
		}
		return sb.toString();
	}
	
	private void saveScript(String scriptStr, String outDir, String outFile) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileOutputStream(new File(outDir + "/" + outFile)));
			out.println(scriptStr);
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		finally {
			out.close();
		}
	}
	
	private String getTableName(String fileName) {
		int lastPos = fileName.lastIndexOf('_');
		int nextToLastPos = fileName.lastIndexOf('_', lastPos - 1);
		String tableName = fileName.substring(0, nextToLastPos);
		return tableName;
	}

}

