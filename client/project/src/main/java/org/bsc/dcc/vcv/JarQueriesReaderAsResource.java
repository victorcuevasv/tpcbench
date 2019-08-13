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

public class JarQueriesReaderAsResource {
	
	private final Map<String, String> ht;
	private final List<String> filesNamesSorted;
	
	public static void main(String[] args) {
		JarQueriesReaderAsResource app = new JarQueriesReaderAsResource(args[0]);
	}
	
	public JarQueriesReaderAsResource(String subDir) {
		System.out.println("Extracting files from jar as resources.");
		List<File> files = this.listFiles(subDir);
		Map<String, String> ht = this.readFiles(files);
		List<String> filesNamesSorted = files.stream().
				map(File::getName).
				map(JarQueriesReaderAsResource::extractNumber).
				sorted().
				map(n -> "query" + n + ".sql").
				collect(Collectors.toList());
		System.out.println("query1.sql :");
		System.out.println(ht.get("query1.sql"));
		for(String s : filesNamesSorted) {
			System.out.println(s);
		}
		this.ht = ht;
		this.filesNamesSorted = filesNamesSorted;
	}

	// Obtain the names with paths of all the .sql files in the jar structure.
	public List<File> listFiles(String subDir) {
		List<File> files = new ArrayList<File>();
		try {
			//URI uri = JarQueriesReaderAsResource.class.getResource("/").toURI();
			URI uri = JarQueriesReaderAsResource.class.getClassLoader().getResource(subDir).toURI();
			Path path = Paths.get(uri);
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (file.toString().endsWith(".sql")) {
						files.add(file.toFile());
					}
					return FileVisitResult.CONTINUE;
				}
			});
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		catch (URISyntaxException use) {
			use.printStackTrace();
		}
		return files;
	}
	
	public Map<String, String> readFiles(List<File> files) {
		Map<String, String> ht = new HashMap<String, String>();
		try {
			for(File file : files) {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(new FileInputStream(file)));
				StringBuilder builder = new StringBuilder();
				String line = "";
				while( (line = reader.readLine()) != null ) {
					builder.append(line + "\n");
				}
				ht.put(file.getName(), builder.toString());
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		return ht;
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


