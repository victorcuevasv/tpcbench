package org.bsc.dcc.vcv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;

public class BigQueryDAO {
	
	private final String project;
    private final String dataset;
    private final BigQuery bigQuery;
    

    public BigQueryDAO(String project, String dataset) {
    	this.project = project;
    	this.dataset = dataset;
    	this.bigQuery = BigQueryOptions.newBuilder().setProjectId(this.project)
				.build().getService();
    }

	public void createTable(String sqlStmt) throws BigQueryException, InterruptedException {
		try {
			QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sqlStmt)
					.setDefaultDataset(this.dataset).build();      
			Job job = this.bigQuery.create(JobInfo.of(config));
			job = job.waitFor();
			if (job.isDone()) {
				System.out.println("Bigquery table created successfully.");
			}
			else {
				System.out.println("Bigquery table was not created.");
			}
		}
		catch (BigQueryException | InterruptedException e) {
			throw e;
		}
	}
	
	private void loadCsvFromGcs(String tableName, String sourceUri, Schema schema) 
			throws BigQueryException, InterruptedException {
		try {
			CsvOptions csvOptions = CsvOptions.newBuilder().
					setFieldDelimiter("\u0001").setEncoding("ISO-8859-1").build();
			TableId tableId = TableId.of(this.dataset, tableName);
			LoadJobConfiguration loadConfig = LoadJobConfiguration
					.newBuilder(tableId, sourceUri, csvOptions).setSchema(schema).build();
			Job job = this.bigQuery.create(JobInfo.of(loadConfig));
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
			throw e;
		}
	}
  
}


