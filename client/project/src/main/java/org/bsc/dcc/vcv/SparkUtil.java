package org.bsc.dcc.vcv;

import scala.collection.immutable.Map;
import scala.collection.*;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;

public class SparkUtil {
	
	public static String stringifySparkConfiguration(SparkSession spark) {
		scala.collection.immutable.Map<String, String> map = spark.conf().getAll();
		scala.collection.Iterator<Tuple2<String, String>> iter = map.iterator();
		StringBuilder sb = new StringBuilder();
		while (iter.hasNext()) {
		    Tuple2<String, String> entry = iter.next();
		    sb.append(entry._1);
		    sb.append('=').append('"');
		    sb.append(entry._2);
		    sb.append('"');
		    if (iter.hasNext()) {
		        sb.append('\n\n');
		    }
		}
		return sb.toString();
	}

}


