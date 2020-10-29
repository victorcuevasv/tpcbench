package org.bsc.dcc.vcv;

public class Partitioning {

public static final String[] tables = {"catalog_returns", "catalog_sales", "inventory", "store_returns",
		"store_sales", "web_returns", "web_sales"};

public static final String[] partKeys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_date_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

public static final String[] distKeys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_date_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

public static final int[] start = {"2450821", "2450815", "2450815", "2450820",
		"2450816", "2450819", "2450816"};

public static final int[] end = {"2452924", "2452654", "2452635", "2452822",
		"2452642", "2453002", "2452642"};

public static final int[] interval = {"5", "5", "5", "5",
		"5", "5", "5"};

}

