package org.bsc.dcc.vcv;

public class QueryRecord {
	
	private int query;
	private int run;
	private long startTime;
	private long endTime;
	private boolean successful;
	private long resultsSize;
	private int tuples;
	
	public QueryRecord() {
		
	}
	
	public QueryRecord(int query) {
		this.query = query;
	}
	
	public void setQuery(int query) {
		this.query = query;
	}
	
	public int getQuery() {
		return this.query;
	}
	
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	
	public long getStartTime() {
		return this.startTime;
	}
	
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	
	public long getEndTime() {
		return this.endTime;
	}
	
	public void setSuccessful(boolean successful) {
		this.successful = successful;
	}
	
	public boolean isSuccessful() {
		return this.successful;
	}
	
	public void setResultsSize(long resultsSize) {
		this.resultsSize = resultsSize;
	}
	
	public long getResultsSize() {
		return this.resultsSize;
	}
	
	public void setTuples(int tuples) {
		this.tuples = tuples;
	}
	
	public int getTuples() {
		return this.tuples;
	}
	
	public void setRun(int run) {
		this.run = run;
	}
	
	public int getRun() {
		return this.run;
	}

}
