package org.bsc.dcc.vcv;

public class QueryRecordConcurrent extends QueryRecord {

	int stream;
	int item;
	
	public QueryRecordConcurrent(int stream, int query) {
		super(query);
		this.stream = stream;
	}
	
	public QueryRecordConcurrent(int stream, int query, int item) {
		super(query);
		this.stream = stream;
		this.item = item;
	}
	
	public int getStream() {
		return this.stream;
	}
	
	public void setStream(int stream) {
		this.stream = stream;
	}
	
	public int getItem() {
		return this.item;
	}
	
	public void setItem(int item) {
		this.item = item;
	}
	
}

