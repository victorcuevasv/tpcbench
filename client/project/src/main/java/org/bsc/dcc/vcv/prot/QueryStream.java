package org.bsc.dcc.vcv.prot;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class QueryStream implements Callable<Void> {

	
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private final int nStream;
	private final ExecuteQueriesConcurrent parent;

	
	public QueryStream(int nStream, BlockingQueue<QueryRecordConcurrent> resultsQueue,
			ExecuteQueriesConcurrent parent) {
		this.nStream = nStream;
		this.resultsQueue = resultsQueue;
		this.parent = parent;
	}

	
	@Override
	public Void call() {
		int[] queries = StreamsTable.matrix[this.nStream];
		for(int i = 0; i < queries.length; i++) {
			this.executeQuery(this.nStream, queries[i], i);
		}
		return null;
	}
	
	
	private void executeQuery(int nStream, int nQuery, int item) {
		QueryRecordConcurrent queryRecord = null;
		queryRecord = new QueryRecordConcurrent(nStream, nQuery);
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

