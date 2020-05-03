package org.bsc.dcc.vcv;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class RunBenchmarkSparkOptions {
	
	private Options options;
	
	public RunBenchmarkSparkOptions() {
		
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
		
		options.addOption( Option.builder().longOpt( "jar-file" )
				.desc( "jar file" )
				.hasArg(true).required(true).build() );
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
		options.addOption( Option.builder().longOpt( "execution-flags" )
				.desc( "execution flags (111111 schema|load|analyze|zorder|power|tput)" )
				.hasArg(true).required(false).type(Integer.class).build() );
	}
	
	public Options getOptions() {
		return this.options;
	}

}


