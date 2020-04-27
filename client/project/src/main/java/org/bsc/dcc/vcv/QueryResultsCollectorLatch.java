package org.bsc.dcc.vcv;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;


public class QueryResultsCollectorLatch implements Runnable {
	
	private final int totalQueries;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final AnalyticsRecorderConcurrent analyticsRecorder;
	private final ConcurrentExecutor parent;
	private final CountDownLatch latch;
	
	public QueryResultsCollectorLatch(int totalQueries, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			AnalyticsRecorderConcurrent analyticsRecorder, ConcurrentExecutor parent,
			CountDownLatch latch) {
		this.totalQueries = totalQueries;
		this.resultsQueue = resultsQueue;
		this.analyticsRecorder = analyticsRecorder;
		this.parent = parent;
		this.latch = latch;
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
		if( this.analyticsRecorder.system.startsWith("snowflake") ) {
			this.parent.saveSnowflakeHistory();
		}
		if( this.parent != null && ! this.analyticsRecorder.system.equalsIgnoreCase("sparkdatabricks") )
			this.parent.closeConnection();
		latch.countDown();
	}
	
}

