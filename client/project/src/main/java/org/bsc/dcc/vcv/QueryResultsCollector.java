package org.bsc.dcc.vcv;

import java.util.concurrent.BlockingQueue;

public class QueryResultsCollector implements Runnable {
	
	private int totalQueries;
	private BlockingQueue<QueryRecord> resultsQueue;
	private AnalyticsRecorder analyticsRecorder;
	private ExecuteQueriesConcurrent parent;
	
	public QueryResultsCollector(int totalQueries, BlockingQueue<QueryRecord> resultsQueue,
			AnalyticsRecorder analyticsRecorder, ExecuteQueriesConcurrent parent) {
		this.totalQueries = totalQueries;
		this.resultsQueue = resultsQueue;
		this.analyticsRecorder = analyticsRecorder;
		this.parent = parent;
	}
	
	public void run() {
		this.analyticsRecorder.header();
		for(int i = 1; i <= this.totalQueries; i++) {
			try {
				QueryRecord queryRecord = resultsQueue.take();
				this.analyticsRecorder.record(queryRecord);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		parent.closeConnection();
	}
	
}

