package org.bsc.dcc.vcv;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class ProcessStreamFiles {

	/**
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory within the main work directory containing the generated query stream .sql files
	 * args[2] name of the output file for the streams order file
	 */
	public static void main(String[] args) {
		if( args.length < 3 ) {
			System.out.println("Incorrect number of arguments.");
			System.exit(1);
		}
		ProcessStreamFiles prog = new ProcessStreamFiles();
		Map<Integer, List<Integer>> ht = prog.processFiles(args[0], args[1]);
		prog.saveStreamTable(ht, args[0], args[2]);
	}

	private Map<Integer, List<Integer>> processFiles(String workDir, String subDir) {
		File directory = new File(workDir + "/" + subDir);
		File[] files = directory.listFiles();
		Map<Integer, List<Integer>> ht = new HashMap<Integer, List<Integer>>();
		for (final File file : files) {
			//It is not to be expected to find directories, but check.
			if ( ! file.isDirectory() ) {
				this.processFile(file, ht);
			}
		}
		return ht;
	}
	
	private void saveStreamTable(Map<Integer, List<Integer>> ht, String workDir, String outFile) {
		try {
		    PrintWriter printWriter = new PrintWriter(new FileWriter(new File(workDir + "/" + outFile)));
		    StringBuilder sb = new StringBuilder();
		    Integer[] queries = ht.keySet().toArray(new Integer[] {});
			Arrays.sort(queries);
			for(int i = 0; i < queries.length; i++) {
				List<Integer> list = ht.get(i);
				for(int j = 0; j < list.size(); j++)
					sb.append(list.get(j) + " ");
				sb.append("\n");
			}
		    printWriter.println(sb.toString());
		    printWriter.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}

	private void processFile(File file, Map<Integer, List<Integer>> ht) {
		BufferedReader inBR = null;
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = null;
			while ((line = inBR.readLine()) != null) {
				//Consider only comment lines starting with '-- start'
				if( ! line.startsWith("-- start") )
					continue;
				this.processLine(line, ht);
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void processLine(String line, Map<Integer, List<Integer>> ht) {
		StringTokenizer tokenizer = new StringTokenizer(line);
		//Ignore '--'
		String dummy = tokenizer.nextToken();
		//Ignore 'start'
		dummy = tokenizer.nextToken();
		//Ignore 'query'
		dummy = tokenizer.nextToken();
		//Get the number of item in the stream (not used since queries follow a natural order)
		int nItem = Integer.parseInt(tokenizer.nextToken());
		//Ignore 'in'
		dummy = tokenizer.nextToken();
		//Ignore 'stream'
		dummy = tokenizer.nextToken();
		//Get the stream number
		int nStream = Integer.parseInt(tokenizer.nextToken());
		//Ignore 'using?
		dummy = tokenizer.nextToken();
		//Ignore 'template'
		dummy = tokenizer.nextToken();
		//Get the query 
		String queryFile = tokenizer.nextToken();
		//Get the query number
		int nQuery = AppUtil.extractNumber(queryFile);
		//Get (or create) the list of queries corresponding to the stream
		List<Integer> list = ht.get(nStream);
		if( list == null ) {
			list = new ArrayList<Integer>();
			ht.put(nStream, list);
		}
		//Add the query number to the list
		list.add(nQuery);
	}

}



