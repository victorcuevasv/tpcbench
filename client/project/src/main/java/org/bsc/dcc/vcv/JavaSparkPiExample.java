package org.bsc.dcc.vcv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JavaSparkPiExample {
	
	private SparkSession spark;
	private static final Logger logger = LogManager.getLogger("AllLog");
	
	public JavaSparkPiExample() {
		this.spark = SparkSession.builder().
				appName("JavaSparkPiExample").
				getOrCreate();
	}
	
	/**
	 * @param args
	 * 
	 * args[0] hostname of the hdfs namenode
	 * args[1] number of slices for the pi calculation task (integer, optional)
	 */
	public static void main(String[] args) {
		JavaSparkPiExample prog = new JavaSparkPiExample();
		int slices = (args.length == 2) ? Integer.parseInt(args[1]) : 2;
		prog.calculatePi(args[0], slices);
	}
	
	private void calculatePi(String namenodeHostname, int slices) {
		try {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}
		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
		int count = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y <= 1) ? 1 : 0;
		}).reduce((integer, integer2) -> integer + integer2);
		String results = "Pi is roughly " + 4.0 * count / n;
		System.out.println(results);
		this.saveResults(results, System.getenv("HOME") + "/pi.txt");
		List<String> resultsList = new ArrayList<String>();
		resultsList.add(results);
		Dataset<Row> df = this.spark.createDataset(resultsList, Encoders.STRING()).toDF();
		this.logger.info("Saving results from the dataframe to HDFS in: " + 
				System.getenv("HOME") + "/pi_dataframe.txt" );
		df.write().mode(SaveMode.Overwrite).text(System.getenv("HOME") + "/pi_dataframe.txt");
		this.logger.info("Saving results from the dataframe to local filesystem in: " + 
				System.getenv("HOME") + "/pi_dataf.txt" );
		this.saveDataset(System.getenv("HOME") + "/pi_dataf.txt", df, true);
//		this.logger.info("Copying file to hdfs: " + "hdfs:// " + namenodeHostname + 
//				System.getenv("HOME") + "/pi.txt");
//		HdfsUtil hdfsUtil = new HdfsUtil();
//		hdfsUtil.copyToHdfs(System.getenv("HOME") + "/pi.txt", 
//				"hdfs://" + namenodeHostname + System.getenv("HOME") + "/pi.txt");
		this.spark.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error in calculatePi");
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}
	
	private void saveResults(String results, String resFileName) {
		try {
			this.logger.info("Saving file to: " + resFileName);
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(results);
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			this.logger.error(ioe);
		}
	}
	
	private void saveDataset(String resFileName, Dataset<Row> dataset, boolean append) {
		try {
			File tmp = new File(resFileName);
			tmp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(resFileName, append);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			List<String> list = dataset.as(Encoders.STRING()).collectAsList();
			//List<String> list = dataset.map(row -> row.mkString(" | "), Encoders.STRING()).collectAsList();
			for(String s: list)
				printWriter.println(s);
			printWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			this.logger.error("Error saving results: " + resFileName);
			this.logger.error(e);
			this.logger.error(AppUtil.stringifyStackTrace(e));
		}
	}

}

