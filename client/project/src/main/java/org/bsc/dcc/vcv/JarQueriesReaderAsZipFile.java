package org.bsc.dcc.vcv;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

public class JarQueriesReaderAsZipFile {
	
	private Map<String, String> ht;
	private List<String> filesNamesSorted;
	
	public static void main(String[] args) {
		JarQueriesReaderAsZipFile app = new JarQueriesReaderAsZipFile(args[0]);
	}
	
	public JarQueriesReaderAsZipFile(String inFile) {
		System.out.println("Extracting files from jar as zip file.");
		this.ht = new HashMap<String, String>();
		List<String> files = this.listFiles(inFile);
		List<String> filesNamesSorted = files.stream().
				map(JarQueriesReaderAsZipFile::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				collect(Collectors.toList());
		System.out.println("query1.sql :");
		System.out.println(ht.get("query1.sql"));
		for(String s : filesNamesSorted) {
			System.out.println(s);
		}
		this.filesNamesSorted = filesNamesSorted;
	}

	// Obtain the names with paths of all the .sql files in the jar structure.
	public List<String> listFiles(String inFile) {
		List<String> files = new ArrayList<String>();
		try {  
			File jarFile = new File(inFile);  
			Map<String, String> zipProperties = new HashMap<>();
			//Reading from an existing zip file, so set to false.
			zipProperties.put("create", "false");
			zipProperties.put("encoding", "UTF-8");
			URI zipFile = URI.create("jar:file:" + jarFile.toPath().toUri().getPath() + "!/");
			FileSystem zipfs = FileSystems.newFileSystem(zipFile, zipProperties);
			Path path = zipfs.getPath("/");
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (file.toString().endsWith(".sql")) {
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
	
	public List<String> getFilesOrdered() {
		return this.filesNamesSorted;
	}
	
	public String getFile(String fileName) {
		return this.ht.get(fileName);
	}
	
	// Converts a string representing a filename like query12.sql to the integer 12.
	public static int extractNumber(String fileName) {
		String nStr = fileName.substring(0, fileName.indexOf('.')).replaceAll("[^\\d.]", "");
		return Integer.parseInt(nStr);
	}

}


