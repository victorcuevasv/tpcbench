package org.bsc.dcc.vcv;

public class QueryRecord {
	
	private int query;
	private long startTime;
	private long endTime;
	private boolean successful;
	
	public QueryRecord() {
		
	}
	
	public QueryRecord(int query) {
		this.query = query;
	}
	
	public void setQuery(int query) {
		this.query = query;
	}
	
	public long getQuery() {
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

}
