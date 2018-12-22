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
	
	private Map<String, String> ht;
	private List<String> filesNamesSorted;
	
	public static void main(String[] args) {
		JarQueriesReaderAsResource app = new JarQueriesReaderAsResource();
	}
	
	public JarQueriesReaderAsResource() {
		List<File> files = this.listFiles();
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
	public List<File> listFiles() {
		List<File> files = new ArrayList<File>();
		try {
			//Get the path from the jar.
			//URI uri = JarQueriesReader.class.getResource("/").toURI();
			//Path path = Paths.get(uri);
			
			//Get the path directly from the file.
			File jarFile = new File("/home/bscuser/workspacedatabricks/benchinf/client/project/target/client-1.0-SNAPSHOT.jar");     
			Path jarPath = jarFile.toPath();
			URI uri = URI.create("jar:file:/" + jarPath.toUri().getPath());
			//Path path = Paths.get(uri);
			
			Map<String, String> zipProperties = new HashMap<>();
			/* We want to read an existing ZIP File, so we set this to False */
			zipProperties.put("create", "false");
			zipProperties.put("encoding", "UTF-8");
			URI zipFile = URI.create("jar:file:" + jarPath.toUri().getPath() + "!/");

			FileSystem zipfs = FileSystems.newFileSystem(zipFile, zipProperties);
			Path path = zipfs.getPath("/");
			
			System.out.println("\n\n\n\n" + path.toAbsolutePath() + "\n\n\n\n");
			
			
			//Path path = new File("/temporal/client-1.0-SNAPSHOT.jar").toPath();
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (file.toString().endsWith(".sql")) {
						System.out.println(file.toString());
						
						String contents = readFile(file);
						System.out.println(contents);
						System.exit(0);
						
						files.add(file.toFile());
					}
					return FileVisitResult.CONTINUE;
				}
			});
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		/*
		catch (URISyntaxException use) {
			use.printStackTrace();
		}
		*/
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


