optimize catalog_returns zorder by cr_returned_date_sk;
optimize catalog_sales zorder by cs_sold_date_sk cs_ship_date_sk;
optimize inventory zorder by inv_date_sk;
optimize store_returns zorder by sr_returned_date_sk;
optimize store_sales zorder by ss_sold_date_sk;
optimize web_returns zorder by wr_returned_date_sk;
optimize web_sales zorder by ws_sold_date_sk ws_ship_date_sk;
