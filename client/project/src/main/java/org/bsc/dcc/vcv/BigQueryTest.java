package org.bsc.dcc.vcv;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;


public class BigQueryTest {
  
	public static void main(String... args) throws Exception {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
                "SELECT "
                    + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                    + "view_count "
                    + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                    + "WHERE tags like '%google-bigquery%' "
                    + "ORDER BY favorite_count DESC LIMIT 10")
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();

    // Print all pages of the results.
    for (FieldValueList row : result.iterateAll()) {
      String url = row.get("url").getStringValue();
      long viewCount = row.get("view_count").getLongValue();
      System.out.printf("url: %s views: %d%n", url, viewCount);
    }
  }
}


