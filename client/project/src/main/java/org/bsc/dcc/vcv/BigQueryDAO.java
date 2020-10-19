package org.bsc.dcc.vcv;

import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

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
    
    //Modifty the SQL create table statement as required by Bigquery, create the table
    //and return the modified SQL.
	public String createTable(String tableName, String sqlStmt) 
			throws BigQueryException, InterruptedException {
		//e.g. varchar(10) -> STRING
		sqlStmt = sqlStmt.replaceAll("varchar\\([0-9]+\\)", "STRING");
		//e.g decimal(5,2) -> FLOAT64
		sqlStmt = sqlStmt.replaceAll("decimal\\([0-9,]+\\)", "FLOAT64");
		//e.g  bigint  ->  INT64 
		sqlStmt = sqlStmt.replaceAll("bigint", "INT64");
		//e.g  int  ->  INT64 
		sqlStmt = sqlStmt.replaceAll("int ", "INT64 ");
		try {
			QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sqlStmt)
					.setDefaultDataset(this.dataset).build();      
			Job job = this.bigQuery.create(JobInfo.of(config));
			job = job.waitFor();
			if (job.isDone()) {
				System.out.println("Table " + tableName + " created successfully.");
			}
			else {
				System.out.println("Error creating table " + tableName + ".");
			}
		}
		catch (BigQueryException | InterruptedException e) {
			throw e;
		}
		return sqlStmt;
	}
	
	public void loadCsvFromGcs(String tableName, String sourceUri, String delimiterCode) 
			throws BigQueryException, InterruptedException {
		try {
			String delimiter = ",";
			if( delimiterCode.equals("SOH") )
				delimiter = "\u0001";
			else if( delimiterCode.equals("PIPE") )
				delimiter = "|";
			CsvOptions csvOptions = CsvOptions.newBuilder().
					setFieldDelimiter(delimiter).setEncoding("ISO-8859-1").build();
			TableId tableId = TableId.of(this.dataset, tableName);
			LoadJobConfiguration loadConfig = LoadJobConfiguration
					.newBuilder(tableId, sourceUri, csvOptions).build();
			Job job = this.bigQuery.create(JobInfo.of(loadConfig));
			// Blocks until this load table job completes its execution, either failing or succeeding.
			job = job.waitFor();
			if (job.isDone()) {
				System.out.println("Table " + tableName + " loaded successfully.");
			}
			else {
				System.out.println(
						"Uable to load into the table " + tableName + " due to an error: "
				+ job.getStatus().getError());
			}
		}
		catch (BigQueryException | InterruptedException e) {
			throw e;
		}
	}
	
	public void countQuery(String tableName) throws Exception {
		String sqlStmt = "SELECT COUNT(*) FROM " + tableName;
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStmt)
				.setDefaultDataset(this.dataset).setUseLegacySql(false).build();
	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = this.bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	    // Wait for the query to complete.
	    queryJob = queryJob.waitFor();
	    // Check for errors
	    if (queryJob == null) {
	      throw new RuntimeException("Job no longer exists");
	    }
	    else if (queryJob.getStatus().getError() != null) {
	      // You can also look at queryJob.getStatus().getExecutionErrors() for all
	      // errors, not just the latest one.
	      throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // Get the results.
	    TableResult result = queryJob.getQueryResults();
	    // Print all pages of the results.
	    for (FieldValueList row : result.iterateAll()) {
	      String nStr = row.get(0).getStringValue();
	      System.out.printf("Number of rows: %s%n", nStr);
	    }
	  }
  
}


