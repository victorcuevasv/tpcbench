package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class FilterKeys {

	private Map<String, String> map;
	
	public FilterKeys() {
		this.map = new HashMap<String, String>();
		map.put("store_sales", "d_year");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
