package org.bsc.dcc.vcv.prot;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class ExecuteQueriesConcurrent implements ConcurrentExecutor {


	private final ExecutorService streamsExecutor;
	private final ExecutorService workersExecutor;
	private final BlockingQueue<QueryRecordConcurrent> queriesQueue;
	private final BlockingQueue<QueryRecordConcurrent> resultsQueue;
	private static final int POOL_SIZE = 150;
	private final int nStreams;
	private final int nWorkers;
	AtomicInteger atomicCounter;
	

	public ExecuteQueriesConcurrent(String[] args) {
		this.nStreams = Integer.parseInt(args[0]);
		this.nWorkers = Integer.parseInt(args[1]);
		this.streamsExecutor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.workersExecutor = Executors.newFixedThreadPool(this.POOL_SIZE);
		this.queriesQueue = new LinkedBlockingQueue<QueryRecordConcurrent>(this.nWorkers * 2);
		this.resultsQueue = new LinkedBlockingQueue<QueryRecordConcurrent>();
		this.atomicCounter = new AtomicInteger(0);
	}
	
	/*
	 * args[0] number of streams
	 * args[1] number of workers
	 */
	public static void main(String[] args) {
		if( args.length != 2 ) {
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
			QueryStream stream = new QueryStream(i, this.queriesQueue, this);
			this.streamsExecutor.submit(stream);
		}
		this.streamsExecutor.shutdown();
		for(int i = 0; i < this.nWorkers; i++) {
			QueryWorker worker = new QueryWorker(i, this.queriesQueue, this.resultsQueue, this, totalQueries);
			this.workersExecutor.submit(worker);
		}
		this.workersExecutor.shutdown();
	}


}

