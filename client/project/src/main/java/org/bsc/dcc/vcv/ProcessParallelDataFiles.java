//package org.bsc.dcc.vcv;

import java.io.File;
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
	 */
	public static void main(String[] args) {
		ProcessParallelDataFiles prog = new ProcessParallelDataFiles();
		Map<String, List<String>> tablesHT = prog.processFiles(args[0]);
		prog.printTablesHT(tablesHT);
	}
	
	public Map<String, List<String>> processFiles(String filesDir) {
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
	
	public void printTablesHT(Map<String, List<String>> tablesHT) {
		Set<String> keySet = tablesHT.keySet();
		for(String table : keySet) {
			List<String> files = tablesHT.get(table);
			System.out.println(table + ":");
			for(String file : files)
				System.out.println("\t" + file);
		}
	}
	
	public String getTableName(String fileName) {
		int lastPos = fileName.lastIndexOf('_');
		int nextToLastPos = fileName.lastIndexOf('_', lastPos - 1);
		String tableName = fileName.substring(0, nextToLastPos);
		return tableName;
	}

}

