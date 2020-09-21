package org.bsc.dcc.vcv;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class ProcessStreamFilesQueries {
	
	/**
	 * 
	 * args[0] main work directory
	 * args[1] subdirectory within the main work directory containing the generated query stream .sql files
	 * args[2] name of the output directory for the streams query files
	 */
	public static void main(String[] args) {
		if( args.length < 3 ) {
			System.out.println("Incorrect number of arguments.");
			System.exit(1);
		}
		ProcessStreamFilesQueries prog = new ProcessStreamFilesQueries();
		prog.processFiles(args[0], args[1], args[2]);
	}
	
	private void processFiles(String workDir, String subDir, String outDir) {
		File directory = new File(workDir + "/" + subDir);
		File[] files = directory.listFiles();
		for (final File file : files) {
			//It is not to be expected to find directories, but check.
			if ( ! file.isDirectory() ) {
				int nStream = AppUtil.extractNumber(file.getName());
				Map<Integer, String> ht = this.processFile(file);
				this.saveQueryStream(ht, workDir, outDir, nStream);
			}
		}
	}

	private Map<Integer, String> processFile(File file) {
		Map<Integer, String> ht = new HashMap<Integer, String>();
		BufferedReader inBR = null;
		try {
			inBR = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = null;
			StringBuilder builder = null;
			int nQuery = -1;
			while ((line = inBR.readLine()) != null) {
				if( line.trim().length() == 0 )
					continue;
				if( line.startsWith("-- start") ) {
					nQuery = this.processStartLine(line);
					builder = new StringBuilder();
				}
				else if( line.startsWith("-- end") ) {
					String query = builder.toString();
					ht.put(nQuery, query);
					builder = null;
					nQuery = -1;
				}
				else {
					builder.append(line + "\n");
				}
			}
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return ht;
	}
	
	private int processStartLine(String line) {
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
		//Ignore 'using'
		dummy = tokenizer.nextToken();
		//Ignore 'template'
		dummy = tokenizer.nextToken();
		//Get the query 
		String queryFile = tokenizer.nextToken();
		//Get the query number
		int nQuery = AppUtil.extractNumber(queryFile);
		return nQuery;
	}
	
	private void saveQueryStream(Map<Integer, String> ht, String workDir, String outDir, int nStream) {
		try {
		    Integer[] queries = ht.keySet().toArray(new Integer[] {});
			Arrays.sort(queries);
			for(int i = 0; i < queries.length; i++) {
				String query = ht.get(queries[i]);
				File outFile = new File(workDir + "/" + outDir + "/stream" + 
					nStream + "/" + "query" + queries[i] + ".sql");
				outFile.getParentFile().mkdirs();
				PrintWriter printWriter = new PrintWriter(new FileWriter(outFile));
				printWriter.println(query);
			    printWriter.close();
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}



