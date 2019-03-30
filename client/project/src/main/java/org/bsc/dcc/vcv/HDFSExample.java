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
		String file = args[1];
		Configuration conf = new Configuration();
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			// Open the path mentioned in HDFS
			in = fs.open(new Path(file));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("End Of file: HDFS file read complete");
		}
		finally {
			IOUtils.closeStream(in);
		}
	}

}

