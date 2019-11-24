package org.bsc.dcc.vcv;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class JarStreamsReaderAsZipFile {
	
	private final Map<String, String> ht;
	private final List<String> filesNames;
	
	public static void main(String[] args) {
		JarStreamsReaderAsZipFile app = new JarStreamsReaderAsZipFile(args[0], args[1]);
	}
	
	public JarStreamsReaderAsZipFile(String inFile, String subDir) {
		System.out.println("Extracting streams from jar as zip file.");
		this.ht = new HashMap<String, String>();
		this.filesNames = this.listFiles(inFile, subDir);
		for(String s : filesNames) {
			System.out.println(s + "\n");
			System.out.println(this.ht.get("s"));
		}
	}

	// Obtain the names with paths of all the .sql files in the jar structure.
	public List<String> listFiles(String inFile, String subDir) {
		//Check if it is an hdfs file
		if( inFile.startsWith("hdfs") ) {
			String outFile = System.getProperty("java.io.tmpdir") + "/tpcdsclusterappqueries.jar";
			HdfsUtil hdfsUtil = new HdfsUtil();
			hdfsUtil.saveToLocal(inFile, outFile);
			inFile = outFile;
		}
		List<String> files = new ArrayList<String>();
		try {  
			File jarFile = new File(inFile);  
			Map<String, String> zipProperties = new HashMap<>();
			//Reading from an existing zip file, so set to false.
			zipProperties.put("create", "false");
			zipProperties.put("encoding", "UTF-8");
			URI zipFile = URI.create("jar:file:" + jarFile.toPath().toUri().getPath() + "!/");
			FileSystem zipfs = FileSystems.newFileSystem(zipFile, zipProperties);
			Path path = zipfs.getPath("/" + subDir);
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (file.toString().endsWith(".txt")) {
						String simpFileName = file.getFileName().toString();
						files.add(simpFileName);
						String contents = readFile(file);
						ht.put(simpFileName, contents);
					}
					return FileVisitResult.CONTINUE;
				}
			});
			zipfs.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return files;
	}
	
	public String readFile(Path file) {
		StringBuilder builder = new StringBuilder();
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(Files.newInputStream(file)));
			String line = "";
			while ((line = reader.readLine()) != null) {
				builder.append(line + "\n");
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	public List<String> getFiles() {
		return this.filesNames;
	}
	
	public String getFile(String fileName) {
		return this.ht.get(fileName);
	}
	
	public int[][] getFileAsMatrix(String fileName) {
		String text = this.ht.get(fileName);
		List<List<Integer>> streamsList = new ArrayList<List<Integer>>();
		int countMax = 0;
		try {
			BufferedReader reader = new BufferedReader(new StringReader(text));
			String line = "";
			while ((line = reader.readLine()) != null) {
				if( line.trim().length() == 0 )
					continue;
				StringTokenizer tokenizer = new StringTokenizer(line);
				List<Integer> list = new ArrayList<Integer>();
				int count = 0;
				while( tokenizer.hasMoreTokens() ) {
					int n = Integer.parseInt(tokenizer.nextToken());
					list.add(n);
					count++;
				}
				streamsList.add(list);
				if( count > countMax )
					countMax = count;
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		int[][] matrix = new int[streamsList.size()][countMax];
		for(int i = 0; i < streamsList.size(); i++) {
			for(int j = 0; j < streamsList.get(i).size(); j++) {
				matrix[i][j] = streamsList.get(i).get(j);
			}
		}
		return matrix;
	}

}


