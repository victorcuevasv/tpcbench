package org.bsc.dcc.vcv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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

	public static void main(String[] args) {
		JavaSparkPiExample prog = new JavaSparkPiExample();
		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		prog.calculatePi(slices);
	}
	
	private void calculatePi(int slices) {
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
		this.spark.stop();
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

}

