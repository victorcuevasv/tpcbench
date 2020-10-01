package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class DistKeys {

	private Map<String, String> map;
	
	public DistKeys() {
		this.map = new HashMap<String, String>();
		map.put("call_center", "all");
		map.put("catalog_page", "all");
		map.put("catalog_returns", "cr_item_sk");
		map.put("catalog_sales", "cs_item_sk");
		map.put("customer", "c_customer_sk");
		map.put("customer_address", "ca_address_sk");
		map.put("customer_demographics", "cd_demo_sk");
		map.put("date_dim", "all");
		map.put("household_demographics", "all");
		map.put("income_band", "all");
		map.put("inventory", "inv_item_sk");
		map.put("item", "i_item_sk");
		map.put("promotion", "all");
		map.put("reason", "all");
		map.put("ship_mode", "all");
		map.put("store", "all");
		map.put("store_returns", "sr_item_sk");
		map.put("store_sales", "ss_item_sk");
		map.put("time_dim", "all");
		map.put("warehouse", "all");
		map.put("web_page", "all");
		map.put("web_returns", "wr_order_number");
		map.put("web_sales", "ws_order_number");
		map.put("web_site", "all");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
