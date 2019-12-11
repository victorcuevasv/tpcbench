package org.bsc.dcc.vcv;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class QueryStreamLimit implements Callable<Void> {

	
	private static final Logger logger = LogManager.getLogger("AllLog");
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final int nStream;
	private final ExecuteQueriesConcurrentLimit parent;

	
	public QueryStreamLimit(int nStream, BlockingQueue<QueryRecordConcurrent> queriesQueue,
			ExecuteQueriesConcurrentLimit parent) {
		this.nStream = nStream;
		this.queriesQueue = queriesQueue;
		this.parent = parent;
	}

	
	@Override
	public Void call() {
		//Integer[] queries = this.queriesHT.keySet().toArray(new Integer[] {});
		//Arrays.sort(queries);
		//this.shuffle(queries);
		//int[] queries = StreamsTable.matrix[this.nStream];
		int[] queries = parent.matrix[this.nStream];
		//int[] impalaKit = {19, 27, 3, 34, 42, 43, 46, 52, 53, 55, 59, 63, 65, 68, 7, 73, 79, 8,  82,  89, 98};
		//Arrays.sort(impalaKit);
		for(int i = 0; i < queries.length; i++) {
			//if( queries[i] == 6 || queries[i] == 9 || queries[i] == 10 || queries[i] == 35 || 
			//		queries[i] == 41 || queries[i] == 66 || queries[i] == 69 || queries[i] == 87 )
			//continue;
			//if( Arrays.binarySearch(impalaKit, queries[i]) < 0 )
			//	continue;
			this.executeQuery(this.nStream, queries[i], i);
			//Add a pause to avoid all of the queries of this stream from filling the queue.
			try {
				TimeUnit.MILLISECONDS.sleep((long)(Math.random() * 10.0));
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	
	private void executeQuery(int nStream, int nQuery, int item) {
		QueryRecordConcurrent queryRecord = new QueryRecordConcurrent(nStream, nQuery, item);
		try {
			this.queriesQueue.put(queryRecord);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
}

