package org.bsc.dcc.vcv;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;

public class CompareResults {

	
	/*
	 * args[0] directory of the results of the first system
	 * args[1] directory of the results of the second system
	 */
	
	public static void main(String[] args) {
		CompareResults app = new CompareResults();
		app.execute(args[0], args[1]);
		
	}
	
	
	private void execute(String dir1, String dir2) {
		List<String> dir1List = listFileNames(dir1);
		List<String> dir2List = listFileNames(dir2);
		for(int i = 1; i <= 99; i++) {
			String dir1FileName = dir1List.get(i - 1);
			String dir2FileName = dir2List.get(i - 1);
			this.compareFiles(dir1 + "/" + dir1FileName, dir2 + "/" + dir2FileName, i);
		}
	}

	
	private List<String> listFileNames(String dir) {
		File dirFile = new File(dir);
		String[] filesArray = dirFile.list(null);
		List<String> filesList = new ArrayList<String>(Arrays.asList(filesArray));
		List<String> filesListSorted = filesList.stream().
				map(JarQueriesReaderAsZipFile::extractNumber).
				sorted().
				map(n -> "query" + n + ".txt").
				collect(Collectors.toList());
		return filesListSorted;
	}
	
	
	private void compareFiles(String fileName1, String fileName2, int nQuery) {
		List<String> results1 = this.readResults(fileName1);
		List<String> results2 = this.readResults(fileName2);
		System.out.println(nQuery + ": " + (results1.size() == results2.size()));
	}
	
	
	public List<String> readResults(String filename) {
		BufferedReader inBR = null;
		List<String> list = new ArrayList<String>();
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line = null;
			while ((line = inBR.readLine()) != null) {
				list.add(line);
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return list.stream().sorted().collect(Collectors.toList());
	}
	
	
}


