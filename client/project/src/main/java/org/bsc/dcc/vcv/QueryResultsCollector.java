package org.bsc.dcc.vcv;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class QueryResultsCollector implements Runnable {
	
	private final int totalQueries;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final AnalyticsRecorderConcurrent analyticsRecorder;
	private final ConcurrentExecutor parent;
	private CountDownLatch latch;
	
	public QueryResultsCollector(int totalQueries, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			AnalyticsRecorderConcurrent analyticsRecorder, ConcurrentExecutor parent,
			CountDownLatch latch) {
		this(totalQueries, resultsQueue, analyticsRecorder, parent);
		this.latch = latch;
	}
	
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
				//Only has an effect on the limited concurrency variant.
				this.parent.incrementAtomicCounter();
				this.analyticsRecorder.record(queryRecord);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.analyticsRecorder.close();
		if( this.analyticsRecorder.system.startsWith("snowflake") && this.parent.getSaveSnowflakeHistory() ) {
			this.parent.saveSnowflakeHistory();
		}
		//if( this.parent != null && ! this.analyticsRecorder.system.equalsIgnoreCase("sparkdatabricks") )
		//	this.parent.closeConnection();
		if( this.latch != null )
			this.latch.countDown();
	}
	
}

