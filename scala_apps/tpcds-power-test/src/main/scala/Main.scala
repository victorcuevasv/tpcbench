import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("Appends Writing Benchmark")
    .enableHiveSupport()
    .getOrCreate()
  val sql = spark.sql(_)
  //val inputURL = "s3://benchmarking-warehouses-1665074575/write_test_tpcds_sf1_1669294980/"
  val inputURL = "s3://benchmarking-datasets-1664540209/write_test_tpcds_sf1_1669294980/"
  val scaleFactor = 1
  val genDataTag = "1669294980"
  val fileFormat = "qbeast"
  val targetDbName = s"write_test_tpcds_sf${scaleFactor}_${genDataTag}"
  val targetLocation = s"s3://benchmarking-datasets-1664540209/${targetDbName}"
  val targetLocation2 = s"s3://benchmarking-warehouses-1665074575/${targetDbName}"
  val targetTable = "store_sales"
  //val runExpTag = "1685120298"
  val runExpTag = args(0).toInt
  val workTable = s"${targetTable}_denorm_${fileFormat}${runExpTag}"
  val workTable2 = s"${targetTable}_denorm2_${fileFormat}${runExpTag}"

  runTest()

  def runTest() = {
    //spark.sparkContext.setLogLevel("DEBUG")
    //spark.sparkContext.setLogLevel("INFO")
    println(s"Creating database ${targetDbName}.")
    //sql(s"DROP DATABASE IF EXISTS ${targetDbName}")
    sql(s"CREATE DATABASE IF NOT EXISTS ${targetDbName}")
    sql(s"USE ${targetDbName}")
    println("Creating temporal view with loaddenormskip data.")
    val dfLoad = spark.read.parquet(f"${inputURL}store_sales_denorm_skip/")
    dfLoad.createOrReplaceTempView("store_sales_denorm_skip")
    println("Creating temporal views with inserts data.")
    createInsertTables(inputURL)
    println("Running loaddenorm skip test.")
    //spark.sparkContext.setLogLevel("DEBUG");
    //createWorkTable(workTable, fileFormat, targetLocation, "WHERE ss_sold_date_sk=2450816")
    //val dfCallCenter = spark.read.parquet("s3://benchmarking-warehouses-1665074575/write_test_tpcds_sf1_1669294980/call_center/")
    //dfCallCenter.collect.foreach(println)
    //spark.sparkContext.setLogLevel("DEBUG");
    createWorkTable(workTable2, fileFormat, targetLocation2, "WHERE ss_sold_date_sk=2450816")
    //println("Running insert test 1.")
    //insertIntoWorkTable(workTable, 1)
  }

  def createInsertTables(inputURL: String) = {
    val dfInsert1 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert1/")
    dfInsert1.createOrReplaceTempView("store_sales_denorm_insert1")
    val dfInsert2 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert2/")
    dfInsert2.createOrReplaceTempView("store_sales_denorm_insert2")
    val dfInsert3 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert3/")
    dfInsert3.createOrReplaceTempView("store_sales_denorm_insert3")
    val dfInsert4 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert4/")
    dfInsert4.createOrReplaceTempView("store_sales_denorm_insert4")
    val dfInsert5 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert5/")
    dfInsert5.createOrReplaceTempView("store_sales_denorm_insert5")
    val dfInsert6 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert6/")
    dfInsert6.createOrReplaceTempView("store_sales_denorm_insert6")
    val dfInsert7 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert7/")
    dfInsert7.createOrReplaceTempView("store_sales_denorm_insert7")
    val dfInsert8 = spark.read.parquet(f"${inputURL}store_sales_denorm_insert8/")
    dfInsert8.createOrReplaceTempView("store_sales_denorm_insert8")
  }

  def createWorkTable(workTable: String, fileFormat: String, targetLocation: String, whereClause: String) = {
    sql(s"""
          CREATE TABLE ${workTable}
        USING ${fileFormat}
        OPTIONS('columnsToIndex'='ss_sold_date_sk,ss_ticket_number', 'cubeSize'='4000000') 
        LOCATION '${targetLocation}/${workTable}'
        AS
        SELECT
        ss_sold_date_sk,
        ss_sold_time_sk,
        ss_item_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_ticket_number,
        ss_quantity,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        d_date_sk,
        d_date_id,
        d_date,
        d_month_seq,
        d_week_seq,
        d_quarter_seq,
        d_year,
        d_dow,
        d_moy,
        d_dom,
        d_qoy,
        d_fy_year,
        d_fy_quarter_seq,
        d_fy_week_seq,
        d_day_name,
        d_quarter_name,
        d_holiday,
        d_weekend,
        d_following_holiday,
        d_first_dom,
        d_last_dom,
        d_same_day_ly,
        d_same_day_lq,
        d_current_day,
        d_current_week,
        d_current_month,
        d_current_quarter,
        d_current_year,
        t_time_sk,
        t_time_id,
        t_time,
        t_hour,
        t_minute,
        t_second,
        t_am_pm,
        t_shift,
        t_sub_shift,
        t_meal_time,
        c_customer_sk,
        c_customer_id,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk,
        c_first_shipto_date_sk,
        c_first_sales_date_sk,
        c_salutation,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_day,
        c_birth_month,
        c_birth_year,
        c_birth_country,
        c_login,
        c_email_address,
        c_last_review_date,
        cd_demo_sk,
        cd_gender,
        cd_marital_status,
        cd_education_status,
        cd_purchase_estimate,
        cd_credit_rating,
        cd_dep_count,
        cd_dep_employed_count,
        cd_dep_college_count,
        hd_demo_sk,
        hd_income_band_sk,
        hd_buy_potential,
        hd_dep_count,
        hd_vehicle_count,
        ca_address_sk,
        ca_address_id,
        ca_street_number,
        ca_street_name,
        ca_street_type,
        ca_suite_number,
        ca_city,
        ca_county,
        ca_state,
        ca_zip,
        ca_country,
        ca_gmt_offset,
        ca_location_type,
        s_store_sk,
        s_store_id,
        s_rec_start_date,
        s_rec_end_date,
        s_closed_date_sk,
        s_store_name,
        s_number_employees,
        s_floor_space,
        s_hours,
        s_manager,
        s_market_id,
        s_geography_class,
        s_market_desc,
        s_market_manager,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,
        s_street_number,
        s_street_name,
        s_street_type,
        s_suite_number,
        s_city,
        s_county,
        s_state,
        s_zip,
        s_country,
        s_gmt_offset,
        s_tax_precentage,
        p_promo_sk,
        p_promo_id,
        p_start_date_sk,
        p_end_date_sk,
        p_item_sk,
        p_cost,
        p_response_target,
        p_promo_name,
        p_channel_dmail,
        p_channel_email,
        p_channel_catalog,
        p_channel_tv,
        p_channel_radio,
        p_channel_press,
        p_channel_event,
        p_channel_demo,
        p_channel_details,
        p_purpose,
        p_discount_active,
        i_item_sk,
        i_item_id,
        i_rec_start_date,
        i_rec_end_date,
        i_item_desc,
        i_current_price,
        i_wholesale_cost,
        i_brand_id,
        i_brand,
        i_class_id,
        i_class,
        i_category_id,
        i_category,
        i_manufact_id,
        i_manufact,
        i_size,
        i_formulation,
        i_color,
        i_units,
        i_container,
        i_manager_id,
        i_product_name
        FROM store_sales_denorm_skip
        ${whereClause}
    """)
  }

  def insertIntoWorkTable(workTable: String, insertNum: Integer) = {
    sql(s"""
        INSERT INTO ${workTable}
        SELECT
        ss_sold_date_sk,
        ss_sold_time_sk,
        ss_item_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_ticket_number,
        ss_quantity,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        d_date_sk,
        d_date_id,
        d_date,
        d_month_seq,
        d_week_seq,
        d_quarter_seq,
        d_year,
        d_dow,
        d_moy,
        d_dom,
        d_qoy,
        d_fy_year,
        d_fy_quarter_seq,
        d_fy_week_seq,
        d_day_name,
        d_quarter_name,
        d_holiday,
        d_weekend,
        d_following_holiday,
        d_first_dom,
        d_last_dom,
        d_same_day_ly,
        d_same_day_lq,
        d_current_day,
        d_current_week,
        d_current_month,
        d_current_quarter,
        d_current_year,
        t_time_sk,
        t_time_id,
        t_time,
        t_hour,
        t_minute,
        t_second,
        t_am_pm,
        t_shift,
        t_sub_shift,
        t_meal_time,
        c_customer_sk,
        c_customer_id,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk,
        c_first_shipto_date_sk,
        c_first_sales_date_sk,
        c_salutation,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_day,
        c_birth_month,
        c_birth_year,
        c_birth_country,
        c_login,
        c_email_address,
        c_last_review_date,
        cd_demo_sk,
        cd_gender,
        cd_marital_status,
        cd_education_status,
        cd_purchase_estimate,
        cd_credit_rating,
        cd_dep_count,
        cd_dep_employed_count,
        cd_dep_college_count,
        hd_demo_sk,
        hd_income_band_sk,
        hd_buy_potential,
        hd_dep_count,
        hd_vehicle_count,
        ca_address_sk,
        ca_address_id,
        ca_street_number,
        ca_street_name,
        ca_street_type,
        ca_suite_number,
        ca_city,
        ca_county,
        ca_state,
        ca_zip,
        ca_country,
        ca_gmt_offset,
        ca_location_type,
        s_store_sk,
        s_store_id,
        s_rec_start_date,
        s_rec_end_date,
        s_closed_date_sk,
        s_store_name,
        s_number_employees,
        s_floor_space,
        s_hours,
        s_manager,
        s_market_id,
        s_geography_class,
        s_market_desc,
        s_market_manager,
        s_division_id,
        s_division_name,
        s_company_id,
        s_company_name,
        s_street_number,
        s_street_name,
        s_street_type,
        s_suite_number,
        s_city,
        s_county,
        s_state,
        s_zip,
        s_country,
        s_gmt_offset,
        s_tax_precentage,
        p_promo_sk,
        p_promo_id,
        p_start_date_sk,
        p_end_date_sk,
        p_item_sk,
        p_cost,
        p_response_target,
        p_promo_name,
        p_channel_dmail,
        p_channel_email,
        p_channel_catalog,
        p_channel_tv,
        p_channel_radio,
        p_channel_press,
        p_channel_event,
        p_channel_demo,
        p_channel_details,
        p_purpose,
        p_discount_active,
        i_item_sk,
        i_item_id,
        i_rec_start_date,
        i_rec_end_date,
        i_item_desc,
        i_current_price,
        i_wholesale_cost,
        i_brand_id,
        i_brand,
        i_class_id,
        i_class,
        i_category_id,
        i_category,
        i_manufact_id,
        i_manufact,
        i_size,
        i_formulation,
        i_color,
        i_units,
        i_container,
        i_manager_id,
        i_product_name
        FROM store_sales_denorm_insert${insertNum}
    """)
  }

}