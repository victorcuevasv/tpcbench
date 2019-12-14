package org.bsc.dcc.vcv.prot;


import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;


public class QueryWorker implements Callable<Void> {

	
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final int nWorker;
	private final int totalQueries;
	private final ExecuteQueriesConcurrent parent;
	private final List<Semaphore> semaphores;

	
	public QueryWorker(int nWorker, BlockingQueue<QueryRecordConcurrent> queriesQueue,
			BlockingQueue<QueryRecordConcurrent> resultsQueue, 
			ExecuteQueriesConcurrent parent, int totalQueries, List<Semaphore> semaphores) {
		this.nWorker = nWorker;
		this.queriesQueue = queriesQueue;
		this.resultsQueue = resultsQueue;
		this.parent = parent;
		this.totalQueries = totalQueries;
		this.semaphores = semaphores;
	}

	
	@Override
	public Void call() {
		while( this.parent.atomicCounter.get() < this.totalQueries  ) {
			try {
				QueryRecordConcurrent queryRecord = this.queriesQueue.poll(10L, TimeUnit.SECONDS);
				if( queryRecord == null )
					break;
				this.executeQuery(queryRecord);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	private void executeQuery(QueryRecordConcurrent queryRecord) {
		try {
			TimeUnit.MILLISECONDS.sleep((long)(Math.random() * 10000.0));
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
			this.semaphores.get(queryRecord.getStream()).release();
		}
	}

	
}

