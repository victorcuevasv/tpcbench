package org.bsc.dcc.vcv;

import java.util.List;
import java.util.ArrayList;
import scala.collection.immutable.Map;
import scala.collection.*;
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {
	
	public static String stringifySparkConfiguration(SparkSession spark) {
		scala.collection.immutable.Map<String, String> map = spark.conf().getAll();
		scala.collection.Iterator<Tuple2<String, String>> iter = map.iterator();
		List<String> list = new ArrayList<String>();
		while (iter.hasNext()) {
		    Tuple2<String, String> entry = iter.next();
		    StringBuilder sb = new StringBuilder();
		    sb.append(entry._1);
		    sb.append('=');
		    sb.append(entry._2);
		    list.add(sb.toString());
		}
		list.sort(String.CASE_INSENSITIVE_ORDER);
		StringBuilder builder = new StringBuilder();
		for(String s : list) {
			builder.append(s + "\n\n");
		}
		return builder.toString();
	}

}


