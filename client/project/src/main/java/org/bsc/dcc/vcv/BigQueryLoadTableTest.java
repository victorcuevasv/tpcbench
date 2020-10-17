package org.bsc.dcc.vcv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;

// Sample to load CSV data from Cloud Storage into a new BigQuery table
public class BigQueryLoadTableTest {
	
  public static void main(String args[]) {
	  BigQueryLoadTableTest app = new BigQueryLoadTableTest();
	  try {
		  app.runLoadCsvFromGcs();
	  }
	  catch(Exception e) {
		  e.printStackTrace();
	  }
  }

  private void runLoadCsvFromGcs() throws Exception {
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "tpcds_synapse_1gb_1_1602805814";
    String tableName = "inventory";
    String sourceUri = "gs://databricks-bsc-benchmark-datasets/1GB/inventory";
    Schema schema =
        Schema.of(
            Field.of("inv_date_sk", StandardSQLTypeName.INT64).
            	toBuilder().setMode(Field.Mode.NULLABLE).build(), //use REQURED for not null
            Field.of("inv_item_sk", StandardSQLTypeName.INT64),
            Field.of("inv_warehouse_sk", StandardSQLTypeName.INT64),
            Field.of("inv_quantity_on_hand", StandardSQLTypeName.INT64));
    loadCsvFromGcs(datasetName, tableName, sourceUri, schema);
  }

  private void loadCsvFromGcs(
      String datasetName, String tableName, String sourceUri, Schema schema) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("databricks-bsc-benchmark").build().getService();    
    	
      // Skip header row in the file.
      //CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
      CsvOptions csvOptions = CsvOptions.newBuilder().
    		  setFieldDelimiter("\u0001").setEncoding("ISO-8859-1").build();

      TableId tableId = TableId.of(datasetName, tableName);
      LoadJobConfiguration loadConfig =
          LoadJobConfiguration.newBuilder(tableId, sourceUri, csvOptions).setSchema(schema).build();

      // Load data from a GCS CSV file into the table
      Job job = bigquery.create(JobInfo.of(loadConfig));
      // Blocks until this load table job completes its execution, either failing or succeeding.
      job = job.waitFor();
      if (job.isDone()) {
        System.out.println("CSV from GCS successfully added during load append job");
      }
      else {
        System.out.println(
            "BigQuery was unable to load into the table due to an error:"
                + job.getStatus().getError());
      }
    }
    catch (BigQueryException | InterruptedException e) {
      System.out.println("Column not added during load append \n" + e.toString());
    }
  }
  
}


