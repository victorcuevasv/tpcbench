package org.bsc.dcc.vcv.prot;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ExecuteQueriesConcurrent implements ConcurrentExecutor {


	private final ExecutorService executor;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 100;
	final private int nStreams;
	

	public ExecuteQueriesConcurrent(String[] args) {
		this.nStreams = Integer.parseInt(args[0]);
		this.executor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
	}
	
	
	public static void main(String[] args) {
		if( args.length != 1 ) {
			System.out.println("Incorrect number of arguments: "  + args.length);
			System.exit(1);
		}
		ExecuteQueriesConcurrent prog = new ExecuteQueriesConcurrent(args);
		prog.executeStreams();
	}
	
	
	private void executeStreams() {
		int nQueries = StreamsTable.matrix[0].length;
		int totalQueries = nQueries * this.nStreams;
		QueryResultsCollector resultsCollector = new QueryResultsCollector(totalQueries, 
				this.resultsQueue, this);
		ExecutorService resultsCollectorExecutor = Executors.newSingleThreadExecutor();
		resultsCollectorExecutor.execute(resultsCollector);
		resultsCollectorExecutor.shutdown();
		for(int i = 0; i < this.nStreams; i++) {
			QueryStream stream = new QueryStream(i, this.resultsQueue, this);
			this.executor.submit(stream);
		}
		this.executor.shutdown();
	}


}

