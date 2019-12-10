package org.bsc.dcc.vcv.prot;

import java.util.concurrent.BlockingQueue;

public class QueryResultsCollector implements Runnable {
	
	private final int totalQueries;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final ConcurrentExecutor parent;
	
	public QueryResultsCollector(int totalQueries, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			 ConcurrentExecutor parent) {
		this.totalQueries = totalQueries;
		this.resultsQueue = resultsQueue;
		this.parent = parent;
	}
	
	public void run() {
		for(int i = 1; i <= this.totalQueries; i++) {
			try {
				QueryRecordConcurrent queryRecord = resultsQueue.take();
				((ExecuteQueriesConcurrent)this.parent).atomicCounter.incrementAndGet();
				System.out.println(queryRecord.toString());
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}

