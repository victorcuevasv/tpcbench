package org.bsc.dcc.vcv;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSExample {

	public static void main(String[] args) throws Exception {
		// File to read in HDFS
		String uri = args[0];
		Configuration conf = new Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", uri);
		// Because of Maven
		//conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		//conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		System.out.println("Configuration: \n" + conf.toString());
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			// Open the path mentioned in HDFS
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("End Of file: HDFS file read complete");
		}
		finally {
			IOUtils.closeStream(in);
		}
	}

}

