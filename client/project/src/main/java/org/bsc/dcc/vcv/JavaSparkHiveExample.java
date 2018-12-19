package org.bsc.dcc.vcv;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class JavaSparkHiveExample {

	public static class Record implements Serializable {
		
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example")
				//.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
				//.config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
				//.config("hive.metastore.warehouse.uris", "thrift://localhost:9083")
				.config("spark.master", "spark://sparkhiveservercontainer:7077")
				//.config("spark.sql.hive.metastore.version", "2.3.0")
				//.config("spark.sql.hive.metastore.jars", "maven")
				.enableHiveSupport()
				//.config("javax.jdo.option.ConnectionURL",
				//          "jdbc:postgresql://postgrescontainer/metastore")
				.getOrCreate();
		
		System.out.println("\n\n\n---------------------------------------");
		System.out.print("spark.sql.hive.metastore.version: ");
		System.out.println(spark.conf().get("spark.sql.hive.metastore.version"));
		System.out.print("spark.sql.hive.metastore.jars: ");
		System.out.println(spark.conf().get("spark.sql.hive.metastore.jars"));
		System.out.println("---------------------------------------\\n\\n\\n");
		
		// Aggregation queries are also supported.
		//spark.sql("SELECT COUNT(*) FROM inventory").show();
		
		spark.sql("SHOW TABLES").show(false);
		System.out.println("\n\n\n---------------------------------------");
		spark.sql("DESCRIBE inventory").show(false);
		System.out.println("\n\n\n---------------------------------------");
		spark.sql("DESCRIBE FORMATTED inventory").show(false);
		System.out.println("\n\n\n---------------------------------------");
		spark.sql("SELECT COUNT(*) FROM inventory").show(false);
		
		/*
		// The results of SQL queries are themselves DataFrames and support all normal
		// functions.
		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

		// The items in DataFrames are of type Row, which lets you to access each column
		// by ordinal.
		Dataset<String> stringsDS = sqlDF.map(
				(MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1), Encoders.STRING());
		stringsDS.show();

		// You can also use DataFrames to create temporary views within a SparkSession.
		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
			Record record = new Record();
			record.setKey(key);
			record.setValue("val_" + key);
			records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");

		// Queries can then join DataFrames data with data stored in Hive.
		spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
		*/

		spark.stop();
	}
}



