package org.bsc.dcc.vcv.prot;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class QueryWorker implements Callable<Void> {

	
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final int nWorker;
	private final int totalQueries;
	private final ExecuteQueriesConcurrent parent;

	
	public QueryWorker(int nWorker, BlockingQueue<QueryRecordConcurrent> queriesQueue,
			BlockingQueue<QueryRecordConcurrent> resultsQueue, 
			ExecuteQueriesConcurrent parent, int totalQueries) {
		this.nWorker = nWorker;
		this.queriesQueue = queriesQueue;
		this.resultsQueue = resultsQueue;
		this.parent = parent;
		this.totalQueries = totalQueries;
	}

	
	@Override
	public Void call() {
		while( this.parent.atomicCounter.get() < this.totalQueries  ) {
			try {
				QueryRecordConcurrent queryRecord = this.queriesQueue.take();
				this.executeQuery(queryRecord);
				this.parent.atomicCounter.incrementAndGet();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	private void executeQuery(QueryRecordConcurrent queryRecord) {
		queryRecord.setStartTime(System.currentTimeMillis());
		try {
			TimeUnit.SECONDS.sleep((long)(Math.random() * 10.0));
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			int tuples = (int)(Math.random() * 100.0);
			queryRecord.setTuples(tuples);
			queryRecord.setResultsSize(tuples * 10);
			queryRecord.setSuccessful(true);
			queryRecord.setEndTime(System.currentTimeMillis());
			this.resultsQueue.add(queryRecord);
		}
	}

	
}

