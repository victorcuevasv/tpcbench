package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class SkipKeys {

	private Map<String, String> map;
	
	public SkipKeys() {
		this.map = new HashMap<String, String>();
		map.put("catalog_sales", "cs_sold_time_sk");
		map.put("store_sales", "ss_sold_time_sk");
		map.put("web_sales", "ws_sold_time_sk");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
