package org.bsc.dcc.vcv;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;


public class CompareResults {

	private final boolean checkColValues;
	private final int checkTopRows;
	
	public CompareResults(boolean checkColValues, int checkTopRows) {
		this.checkColValues = checkColValues;
		this.checkTopRows = checkTopRows;
	}
	
	/*
	 * args[0] directory of the results of the first system
	 * args[1] directory of the results of the second system
	 * args[2] left range of the queries (1..99) default 1
	 * args[3] right range of the queries (1..99) default 99
	 * args[4] check column values (true/false) default false
	 * args[5] check top n rows (-1, 1..n) default 100, negative value to ignore
	 */
	
	public static void main(String[] args) {
		int left = args.length > 2 ? Integer.parseInt(args[2]) : 1;
		int right = args.length > 3 ? Integer.parseInt(args[3]) : 99;
		boolean checkColValues =  args.length > 4 ? Boolean.parseBoolean(args[4]) : false;
		int checkTopRows = args.length > 5 ? Integer.parseInt(args[5]) : 100;
		CompareResults app = new CompareResults(checkColValues, checkTopRows);
		app.execute(args[0], args[1], left, right);
		
	}
	
	
	private void execute(String dir1, String dir2, int left, int right) {
		List<String> dir1List = listFileNames(dir1);
		List<String> dir2List = listFileNames(dir2);
		for(int i = left; i <= right; i++) {
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
		System.out.println("Tuple count for query " + nQuery + 
					": 1 -> " + results1.size() + " 2 -> " + results2.size());
		for(int i = 0; i < results1.size(); i++) {
			if( this.checkTopRows == 0 )
				break;
			else if( this.checkTopRows > -1 && (i + 1) > this.checkTopRows )
				break;
			this.compareLines(results1.get(i), results2.get(i), i + 1);
		}
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
	
	
	private void compareLines(String line1, String line2, int n) {
		System.out.println("Comparing result " + n);
		List<String> cols1 = this.lineToList(line1);
		List<String> cols2 = this.lineToList(line2);
		boolean linesEq = true;
		if( cols1.size() != cols2.size() ) {
			System.out.println("Different column sizes: 1 -> " + cols1.size() + " 2 -> " + cols2.size() );
			linesEq = false;
		}
		else {
			for(int i = 0; i < cols1.size(); i++) {
				String s1 = cols1.get(i);
				String s2 = cols2.get(i);
				if( s1.equals(s2) )
					;
				else {
					boolean s1isNum = this.isNumeric(s1);
					boolean s2isNum = this.isNumeric(s2);
					if( s1isNum && s2isNum && this.checkColValues) {
						double s1d = Double.parseDouble(s1);
						double s2d = Double.parseDouble(s2);
						if( this.compare(s1d, s2d, 0.1) )
							;
						else {
							linesEq = false;
						}
					}
					else if(this.checkColValues)
						linesEq =  false;
				}
			} 
		}
		if( ! linesEq ) {
			System.out.println("Different column values.");
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
	
	
	private boolean compare(double a, double b, double epsilon) {
		if ( a == b)
			return true;
		return Math.abs(a - b) < epsilon;
	}
	
	
	public static boolean isNumeric(String strNum) {
		try {
	    	double d = Double.parseDouble(strNum);
	    }
	    catch(NumberFormatException | NullPointerException nfe) {
	    	return false;
	    }
	    return true;
	}

	
}


