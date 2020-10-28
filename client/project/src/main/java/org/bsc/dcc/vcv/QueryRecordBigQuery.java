package org.bsc.dcc.vcv;

public class QueryRecordBigQuery extends QueryRecord {
	
	private long bytesBilled;
	
	public QueryRecordBigQuery() {
		super();
	}
	
	public QueryRecordBigQuery(int query) {
		super(query);
	}
	
	public void setBytesBilled(long bytesBilled) {
		this.bytesBilled = bytesBilled;
	}
	
	public long getBytesBilled() {
		return this.bytesBilled;
	}

}
