package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class FilterValues {

	private Map<String, String> map;
	
	public FilterValues() {
		this.map = new HashMap<String, String>();
		map.put("store_sales", "2000");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
