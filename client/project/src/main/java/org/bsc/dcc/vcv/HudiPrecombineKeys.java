package org.bsc.dcc.vcv;

import java.util.Map;
import java.util.HashMap;

public class HudiPrecombineKeys {

	private Map<String, String> map;
	
	public HudiPrecombineKeys() {
		this.map = new HashMap<String, String>();
		map.put("call_center", "cc_call_center_id");
		map.put("catalog_page", "cp_catalog_page_id");
		map.put("catalog_returns", "cr_returned_time_sk");
		map.put("catalog_sales", "cs_sold_time_sk");
		map.put("customer", "c_customer_id");
		map.put("customer_address", "ca_address_id");
		map.put("customer_demographics", "cd_dep_count");
		map.put("date_dim", "d_date_id");
		map.put("dbgen_version", "dv_create_time");
		map.put("household_demographics", "hd_dep_count");
		map.put("income_band", "ib_upper_bound");
		map.put("inventory", "inv_quantity_on_hand");
		map.put("item", "i_item_id");
		map.put("promotion", "p_promo_id");
		map.put("reason", "r_reason_id");
		map.put("ship_mode", "sm_ship_mode_id");
		map.put("store", "s_store_id");
		map.put("store_returns", "sr_return_time_sk");
		map.put("store_sales", "ss_sold_time_sk");
		map.put("time_dim", "t_time_id");
		map.put("warehouse", "w_warehouse_id");
		map.put("web_page", "wp_web_page_id");
		map.put("web_returns", "wr_returned_time_sk");
		map.put("web_sales", "ws_sold_time_sk");
		map.put("web_site", "web_site_id");
	}
	
	public Map<String, String> getMap() {
		return this.map;
	}
	
}
