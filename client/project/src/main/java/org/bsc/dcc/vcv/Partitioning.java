package org.bsc.dcc.vcv;

public class Partitioning {

public static final String[] tables = {"catalog_returns", "catalog_sales", "inventory", "store_returns",
		"store_sales", "web_returns", "web_sales"};

public static final String[] keys = {"cr_returned_date_sk", "cs_sold_date_sk", "inv_date_sk", "sr_returned_date_sk",
		"ss_sold_date_sk", "wr_returned_date_sk", "ws_sold_date_sk"};

}


/* At 1 TB.
 * describe formatted catalog_returns cr_returned_date_sk: [distinct_count,2031]
 * describe formatted catalog_sales cs_sold_date_sk: [distinct_count,1772]
 * describe formatted inventory inv_date_sk: [distinct_count,267]
 * describe formatted store_returns sr_returned_date_sk: [distinct_count,1943
 * describe formatted store_sales ss_sold_date_sk: [distinct_count,1781
 * describe formatted web_returns wr_returned_date_sk: [distinct_count,2121]
 * describe formatted web_sales ws_sold_date_sk: [distinct_count,1781]
 * 
 * At 1 GB.
 * min cr_returned_date_sk = 1, max cr_returned_date_sk = 9996
 */
   