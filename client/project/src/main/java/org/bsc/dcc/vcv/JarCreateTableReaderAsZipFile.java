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

public class JarCreateTableReaderAsZipFile {
	
	private Map<String, String> ht;
	private List<String> filesNames;
	
	public static void main(String[] args) {
		JarCreateTableReaderAsZipFile app = new JarCreateTableReaderAsZipFile(args[0], args[1]);
	}
	
	public JarCreateTableReaderAsZipFile(String inFile, String subDir) {
		System.out.println("Extracting files from jar as zip file.");
		this.ht = new HashMap<String, String>();
		List<String> files = this.listFiles(inFile, subDir);
		for(String s : files) {
			System.out.println(s);
		}
		this.filesNames = files;
	}

	// Obtain the names with paths of all the .sql files in the jar structure.
	public List<String> listFiles(String inFile, String subDir) {
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
	
	public List<String> getFiles() {
		return this.filesNames;
	}
	
	public String getFile(String fileName) {
		return this.ht.get(fileName);
	}

}


