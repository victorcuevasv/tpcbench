package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class ClusterByKeys {

	private Map<String, String> map;
	
	public ClusterByKeys() {
		this.map = new HashMap<String, String>();
		map.put("call_center", "cc_call_center_sk");
		map.put("catalog_page", "cp_catalog_page_sk");
		map.put("catalog_returns", "cr_returned_date_sk");
		map.put("catalog_sales", "cs_sold_date_sk");
		map.put("customer", "c_customer_sk");
		map.put("customer_address", "ca_address_sk");
		map.put("customer_demographics", "cd_demo_sk");
		map.put("date_dim", "d_date_sk");
		map.put("household_demographics", "hd_demo_sk");
		map.put("income_band", "ib_income_band_sk");
		map.put("inventory", "inv_date_sk");
		map.put("item", "i_item_sk");
		map.put("promotion", "p_promo_sk");
		map.put("reason", "r_reason_sk");
		map.put("ship_mode", "sm_ship_mode_sk");
		map.put("store", "s_store_sk");
		map.put("store_returns", "sr_returned_date_sk");
		map.put("store_sales", "ss_sold_date_sk");
		map.put("time_dim", "t_time_sk");
		map.put("warehouse", "w_warehouse_sk");
		map.put("web_page", "wp_web_page_sk");
		map.put("web_returns", "wr_returned_date_sk");
		map.put("web_sales", "ws_sold_date_sk");
		map.put("web_site", "web_site_sk");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
