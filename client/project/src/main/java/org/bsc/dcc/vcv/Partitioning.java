package org.bsc.dcc.vcv;

public class Partitioning {

public static final String[] tables = {"catalog_returns", "catalog_sales", "inventory", "store_returns",
		"store_sales", "web_returns", "web_sales"};

public static final String[] partKeys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_date_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

public static final String[] distKeys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_date_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

public static final String[] bucketKeys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_time_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

}

