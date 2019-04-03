package org.bsc.dcc.vcv;

import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtil {
	
	private static final Logger logger = LogManager.getLogger("AllLog");
	
	public HdfsUtil() {
		
	}
	
	public void saveToLocal(String uri, String outFile) {
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", uri);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		System.out.println("Configuration: \n" + conf.toString());
		// Get the filesystem - HDFS
		FSDataInputStream in = null;
		java.io.FileOutputStream out = null;
		try {
			// Open the path mentioned in HDFS
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			out = new java.io.FileOutputStream(new java.io.File(outFile));
			byte buffer[] = new byte[256];
			int bytesRead = 0;
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytesRead);
			}
			out.close();
			System.out.println("End Of file: HDFS file saving complete");
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error(e);
			System.exit(1);
		}
		finally {
			IOUtils.closeStream(in);
		}
	}

}

