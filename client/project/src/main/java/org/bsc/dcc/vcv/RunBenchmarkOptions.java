package org.bsc.dcc.vcv;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class RunBenchmarkOptions {
	
	private Options options;
	
	public RunBenchmarkOptions() {
		
		this.options = new Options();
		options.addOption( Option.builder().longOpt( "main-work-dir" )
                .desc( "main work directory" ).hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "schema-name" )
			    .desc( "schema name" ).hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "results-dir" )
				.desc( "results directory" ).hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "experiment-name" )
				.desc( "experiment name (name of subdir within the results dir)" )
				.hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "system-name" )
				.desc( "system name (system name used within the logs)" )
				.hasArg(true).required(true).build() );
		
		options.addOption( Option.builder().longOpt( "instance-number" )
				.desc( "experiment instance number" )
				.hasArg(true).required(true).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "override-load-system" )
				.desc( "override the default system to use for data loading" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "raw-data-dir" )
				.desc( "directory for generated data raw files" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "create-table-dir" )
				.desc( "subdirectory within the jar that contains the create table files" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "text-file-suffix" )
				.desc( "suffix used for intermediate table text files" )
				.hasArg(true).required(false).build() );
		
		options.addOption( Option.builder().longOpt( "ext-raw-data-location" )
				.desc( "prefix of external location for raw data files" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "ext-tables-location" )
				.desc( "prefix of external location for tables" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "table-format" )
				.desc( "format for tables column-storage" )
				.hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "count-queries" )
				.desc( "whether to run queries to count the tuples generated" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "use-partitioning" )
				.desc( "whether to use data partitioning for the tables" )
				.hasArg(true).required(true).type(Boolean.class).build() );
		
		options.addOption( Option.builder().longOpt( "all-or-create-file" )
				.desc( "\"all\" or create table file" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "use-bucketing" )
				.desc( "whether to use data partitioning for the tables" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "server-hostname" )
				.desc( "server hostname" )
				.hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "connection-username" )
				.desc( "connection username" )
				.hasArg(true).required(true).build() );
		options.addOption( Option.builder().longOpt( "jar-file" )
				.desc( "jar file" )
				.hasArg(true).required(true).build() );
		
		options.addOption( Option.builder().longOpt( "override-analyze-system" )
				.desc( "override the default system to use for table statistics" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "use-row-stats" )
				.desc( "whether to generate row statistics by analyzing tables" )
				.hasArg(true).required(true).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "use-column-stats" )
				.desc( "whether to generate column statistics by analyzing tables" )
				.hasArg(true).required(true).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "tpcds-test" )
				.desc( "indicate a specific test to run when needed" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "queries-dir-in-jar" )
				.desc( "queries dir within the jar" )
				.hasArg(true).required(false).build() );
		
		options.addOption( Option.builder().longOpt( "results-subdir" )
				.desc( "subdirectory of work directory to store the results" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "plans-subdir" )
				.desc( "subdirectory of work directory to store the execution plans" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "save-power-plans" )
				.desc( "save power test plans" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "save-power-results" )
				.desc( "save power test results" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "all-or-query-file" )
				.desc( "\"all\" or query file" )
				.hasArg(true).required(true).build() );
		
		options.addOption( Option.builder().longOpt( "save-tput-plans" )
				.desc( "save tput test plans" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "save-tput-results" )
				.desc( "save tput test results" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "number-of-streams" )
				.desc( "number of streams for the throughput test" )
				.hasArg(true).required(true).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "random-seed" )
				.desc( "random seed" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "multiple-connections" )
				.desc( "use multiple connections" )
				.hasArg(true).required(false).type(Integer.class).build() );
		
		options.addOption( Option.builder().longOpt( "tput-changing-streams" )
				.desc( "use queries specific to a given stream" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "cluster-id" )
				.desc( "identifier of the cluster to use to evaluate queries" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "raw-column-delimiter" )
				.desc( "delimiter for the columns in the raw data (SOH, PIPE) default SOH" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "power-test-runs" )
				.desc( "number of runs to perform for the power test (default 1)" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "db-password" )
				.desc( "database password" )
				.hasArg(true).required(false).build() );
		
		options.addOption( Option.builder().longOpt( "num-cores" )
				.desc( "number of cores in the cluster to set the number of shuffle partitions" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "ordered-clustering" )
				.desc( "use ordered clustering for some tables in synapse" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "use-distribute-keys" )
				.desc( "use distribute by keys in redshift and synapse" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "result-tuples-limit-tput" )
				.desc( "limit for the tuples to retrieve from the results of a query for the tput test" )   
				.hasArg(true).required(false).type(Long.class).build() );
		
		options.addOption( Option.builder().longOpt( "partition-with-distribute-by" )
				.desc( "use a distribute by clause for partitioning" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "denorm-with-filter" )
				.desc( "use a filter attribute and value for denormalization" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		options.addOption( Option.builder().longOpt( "denorm-all-or-file" )
				.desc( "\"all\" or create table file for denorm tables" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "scale-factor" )
				.desc( "scale factor used to run the benchmark" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "denorm-apply-skip" )
				.desc( "skip data to be inserted from denorm tables" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		
		options.addOption( Option.builder().longOpt( "analyze-zorder-all-or-file" )
				.desc( "\"all\" or query file for denorm analyze and z-order" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "gdpr-customer-sk" )
				.desc( "customer surrogate key for the gdpr test" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "read-instance" )
				.desc( "instance number of the read test" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "datesk-gt-threshold" )
				.desc( "greater than threshold for the date-sk attribute to generate update data" )
				.hasArg(true).required(false).build() );
		options.addOption( Option.builder().longOpt( "force-sequential-runs" )
				.desc( "force power test runs to be of the form q1, q1, q2, q2,...qn, qn" )
				.hasArg(true).required(false).type(Boolean.class).build() );
		
		options.addOption( Option.builder().longOpt( "execution-flags" )
				.desc( "execution flags (111111 schema|load|analyze|zorder|power|tput)" )
				.hasArg(true).required(false).type(Integer.class).build() );
		options.addOption( Option.builder().longOpt( "synapse-token" )
				.desc( "storage account SAS toke for synapse import" )
				.hasArg(true).required(false).build() );		
	}
	
	public Options getOptions() {
		return this.options;
	}

}


