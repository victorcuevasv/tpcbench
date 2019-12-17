package org.bsc.dcc.vcv;

public class QueryRecordConcurrentLimit extends QueryRecordConcurrent {

	private long queueStartTime;
	private long queueEndTime;
	
	public QueryRecordConcurrentLimit(int stream, int query) {
		super(stream, query);
	}
	
	public QueryRecordConcurrentLimit(int stream, int query, int item) {
		super(stream, query);
		this.item = item;
	}
	
	public long getQueueStartTime() {
		return this.queueStartTime;
	}
	
	public void setQueueStartTime(long queueStartTime) {
		this.queueStartTime = queueStartTime;
	}
	
	public long getQueueEndTime() {
		return this.queueEndTime;
	}
	
	public void setQueueEndTime(long queueEndTime) {
		this.queueEndTime = queueEndTime;
	}
	
}

