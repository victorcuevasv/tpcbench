package org.bsc.dcc.vcv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;

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
					.setDefaultDataset("tpcds_synapse_1gb_1_1602805814").build();      
			Job job = bigquery.create(JobInfo.of(config));
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
  
  
}


