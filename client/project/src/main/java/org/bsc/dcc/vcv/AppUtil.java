package org.bsc.dcc.vcv;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AppUtil {
	
	public static String stringifyStackTrace(Exception e) {
	    StringWriter sw = new StringWriter();
	    PrintWriter pw = new PrintWriter(sw);
	    e.printStackTrace(pw);
	    String stackTrace = sw.toString();
	    return stackTrace;
	}
	
}


