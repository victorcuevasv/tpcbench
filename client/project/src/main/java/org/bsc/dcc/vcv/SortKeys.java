package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class SortKeys {

	private Map<String, String> map;
	
	public SortKeys() {
		this.map = new HashMap<String, String>();
		map.put("call_center", "none");
		map.put("catalog_page", "none");
		map.put("catalog_returns", "cr_returned_date_sk");
		map.put("catalog_sales", "cs_sold_date_sk");
		map.put("customer", "none");
		map.put("customer_address", "none");
		map.put("customer_demographics", "none");
		map.put("date_dim", "none");
		map.put("household_demographics", "none");
		map.put("income_band", "none");
		map.put("inventory", "inv_date_sk");
		map.put("item", "i_category");
		map.put("promotion", "none");
		map.put("reason", "none");
		map.put("ship_mode", "none");
		map.put("store", "none");
		map.put("store_returns", "sr_returned_date_sk");
		map.put("store_sales", "ss_sold_date_sk");
		map.put("time_dim", "none");
		map.put("warehouse", "none");
		map.put("web_page", "none");
		map.put("web_returns", "wr_returned_date_sk");
		map.put("web_sales", "ws_sold_date_sk");
		map.put("web_site", "none");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
