package org.bsc.dcc.vcv;

public interface ConcurrentExecutor {

	public void closeConnection();
	
	public void saveSnowflakeHistory();
	
	public boolean getSaveSnowflakeHistory();
	
	public void incrementAtomicCounter();
	
}
