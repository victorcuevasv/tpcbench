package org.bsc.dcc.vcv;

import java.io.PrintWriter;
import java.io.StringWriter;

import scala.collection.immutable.Map;
import scala.collection.*;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;

public class AppUtil {
	
	public static String stringifyStackTrace(Exception e) {
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    e.printStackTrace(pw);
	    String stackTrace = sw.toString();
	    return stackTrace;
	}
	
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
		        sb.append('\n');
		    }
		}
		return sb.toString();
	}

}


