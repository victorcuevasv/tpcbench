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
            //+ "`"
            //+ projectId
            //+ "."
            //+ datasetId
            //+ "."
            + "inventory"
            //+ "`"
            + " ( \n"
            + "inv_date_sk               INT64               not null,\n"
            + "inv_item_sk               INT64               not null,\n"
            + "inv_warehouse_sk          INT64               not null,\n"
            + "inv_quantity_on_hand      INT64                       ,\n"
            + ");";
    execDdl(ddl);
  }

  private void execDdl(String ddl) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("databricks-bsc-benchmark").getDefaultInstance().getService();

      QueryJobConfiguration config = QueryJobConfiguration.newBuilder(ddl).setDefaultDataset("tpcds_synapse_1gb_1_1602805814").build();

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

/*
 *  String projectId = "my_project_id";
 String datasetName = "my_dataset_name";
 DatasetId datasetId = DatasetId.of(projectId, datasetName);
 Dataset dataset = bigquery.getDataset(datasetId);
 
 *
 *
 *QueryJobConfiguration.getDefaultDataset
public DatasetId getDefaultDataset()
Returns the default dataset. This dataset is used for all unqualified table names used in the query.
 *
 *
 *
 *
 */



