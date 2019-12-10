package org.bsc.dcc.vcv.prot;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;


public class QueryStream implements Callable<Void> {

	
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final int nStream;
	private final ExecuteQueriesConcurrent parent;

	
	public QueryStream(int nStream, BlockingQueue<QueryRecordConcurrent> queriesQueue,
			ExecuteQueriesConcurrent parent) {
		this.nStream = nStream;
		this.queriesQueue = queriesQueue;
		this.parent = parent;
	}

	
	@Override
	public Void call() {
		int[] queries = StreamsTable.matrix[this.nStream];
		for(int i = 0; i < queries.length; i++ ) {
			this.executeQuery(this.nStream, queries[i], i);
		}
		return null;
	}
	
	
	private void executeQuery(int nStream, int nQuery, int item) {
		QueryRecordConcurrent queryRecord = new QueryRecordConcurrent(nStream, nQuery);
		try {
			this.queriesQueue.put(queryRecord);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	
}

