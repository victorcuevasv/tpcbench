package org.bsc.dcc.vcv;

public class QueryRecordConcurrent extends QueryRecord {

	int stream;
	
	public QueryRecordConcurrent(int stream, int query) {
		super(query);
		this.stream = stream;
	}
	
	public int getStream() {
		return this.stream;
	}
	
	public void setStream(int stream) {
		this.stream = stream;
	}
	
}

