package org.bsc.dcc.vcv;

import java.util.concurrent.BlockingQueue;

public class QueryResultsCollector implements Runnable {
	
	private final int totalQueries;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final AnalyticsRecorderConcurrent analyticsRecorder;
	private final ConcurrentExecutor parent;
	
	public QueryResultsCollector(int totalQueries, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			AnalyticsRecorderConcurrent analyticsRecorder, ConcurrentExecutor parent) {
		this.totalQueries = totalQueries;
		this.resultsQueue = resultsQueue;
		this.analyticsRecorder = analyticsRecorder;
		this.parent = parent;
	}
	
	public void run() {
		this.analyticsRecorder.header();
		for(int i = 1; i <= this.totalQueries; i++) {
			try {
				QueryRecordConcurrent queryRecord = resultsQueue.take();
				this.analyticsRecorder.record(queryRecord);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if( this.parent != null && ! this.analyticsRecorder.system.equalsIgnoreCase("sparkdatabricks"))
			this.parent.closeConnection();
	}
	
}

