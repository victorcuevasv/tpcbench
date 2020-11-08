package org.bsc.dcc.vcv;

public class QueryRecordConcurrentBigQuery extends QueryRecordConcurrent {

	private long bytesBilled;
	
	public QueryRecordConcurrentBigQuery(int stream, int query) {
		super(stream, query);
	}
	
	public QueryRecordConcurrentBigQuery(int stream, int query, int item) {
		super(stream, query, item);
	}
	
	public void setBytesBilled(long bytesBilled) {
		this.bytesBilled = bytesBilled;
	}
	
	public long getBytesBilled() {
		return this.bytesBilled;
	}
	
}

