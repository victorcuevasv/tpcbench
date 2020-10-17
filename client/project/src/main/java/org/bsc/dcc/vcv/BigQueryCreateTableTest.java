package org.bsc.dcc.vcv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;

// Sample to create a view using DDL
public class BigQueryCreateTableTest {
	
  public static void main(String args[]) {
	  BigQueryCreateTableTest app = new BigQueryCreateTableTest();
	  app.runDDLCreateTable();
  }

  private void runDDLCreateTable() {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "databricks-bsc-benchmark";
    String datasetId = "tpcds_synapse_1gb_1_1602805814";
    String ddl =
        "CREATE TABLE "
            + "`"
            + projectId
            + "."
            + datasetId
            + "."
            + "call_center"
            + "`"
            + " ( \n"
            + "inv_date_sk               integer               not null,\n"
            + "inv_item_sk               integer               not null,\n"
            + "inv_warehouse_sk          integer               not null,\n"
            + "inv_quantity_on_hand      integer                       ,\n"
            + "primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)\n"
            + ");";
    execDdl(ddl);
  }

  private void execDdl(String ddl) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      QueryJobConfiguration config = QueryJobConfiguration.newBuilder(ddl).build();

      // create a view using query and it will wait to complete job.
      Job job = bigquery.create(JobInfo.of(config));
      job = job.waitFor();
      if (job.isDone()) {
        System.out.println("View created successfully");
      }
      else {
        System.out.println("Table was not created");
      }
    }
    catch (BigQueryException | InterruptedException e) {
      System.out.println("Table was not created. \n" + e.toString());
    }
  }
  
  
}


