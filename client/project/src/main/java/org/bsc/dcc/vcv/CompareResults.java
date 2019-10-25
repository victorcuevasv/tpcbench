package org.bsc.dcc.vcv;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;


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
			this.compareFiles(dir1, dir1FileName, dir2, dir2FileName, i);
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
	
	
	private void compareFiles(String dirName1, String fileName1, String dirName2, String fileName2, int nQuery) {
		List<String> results1 = this.readResults(dirName1 + "/" + fileName1);
		List<String> results2 = this.readResults(dirName2 + "/" + fileName2);
		String fileName1NoExt = fileName1.substring(0, fileName1.indexOf('.'));
		String fileName2NoExt = fileName2.substring(0, fileName2.indexOf('.'));
		this.saveProcessedResults(results1, dirName1 + "/" + fileName1NoExt + "_p.txt");
		this.saveProcessedResults(results2, dirName2 + "/" + fileName2NoExt + "_p.txt");
		if( results1.size() != results2.size() )
			System.out.println("Different result tuple count for query " + nQuery + 
					": 1 -> " + results1.size() + " 2 -> " + results2.size());
		else
			System.out.println("Equal result tuple count for query " + nQuery + 
					": 1 -> " + results1.size() + " 2 -> " + results2.size());
		/*
		int minSize = (int)Math.min(results1.size(), results2.size());
		for(int i = 0; i < minSize; i++) {
			this.compareLines(results1.get(i), results2.get(i));
		}
		*/
	}
	
	
	public List<String> readResults(String filename) {
		BufferedReader inBR = null;
		List<String> list = new ArrayList<String>();
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			String line = null;
			while ((line = inBR.readLine()) != null) {
				list.add(processLine(line));
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return list.stream().sorted().collect(Collectors.toList());
	}
	
	
	public void saveProcessedResults(List<String> list, String filename) {
		BufferedWriter outBW = null;
		try {
			outBW = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)));
			for(String s : list) {
				outBW.write(s);
				outBW.newLine();
			}
			outBW.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	
	private String processLine(String line) {
		StringTokenizer tokenizer = new StringTokenizer(line, "|");
		StringBuilder builder = new StringBuilder();
		boolean first = true;
		while(tokenizer.hasMoreTokens()) {
			if( ! first )
				builder.append(" | ");
			first = false;
			String token = tokenizer.nextToken();
			if( token.trim().length() == 0 )
				token = "null";
			builder.append(token.trim());
		}
		return builder.toString();
	}
	
	
	private void compareLines(String line1, String line2) {
		List<String> cols1 = this.lineToList(line1);
		List<String> cols2 = this.lineToList(line2);
		if( cols1.size() != cols2.size() ) {
			System.out.println("Different column sizes: 1 -> " + cols1.size() + " 2 -> " + cols2.size() );
			System.out.println("Line 1: " + line1);
			System.out.println("Line 2: " + line2);
		}
	}
	
	
	private List<String> lineToList(String line) {
		StringTokenizer tokenizer = new StringTokenizer(line, "|");
		List<String> list = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			list.add(token.trim());
		}
		return list;
	}

	
}


