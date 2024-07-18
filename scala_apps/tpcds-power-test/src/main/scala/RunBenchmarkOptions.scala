import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

class RunBenchmarkOptions {
  
    val options: Options = new Options()
	
	def initOptions() = {

        options.addOption( Option.builder().longOpt("exec-flags")
                .desc("Flags for the execution of different tests create_db|load|power")
                .hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("scale-factor")
				.desc("Scale factor in GB for the benchmark")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("raw-base-url")
				.desc("URL of the input raw TPC-DS CSV data")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("warehouse-base-url")
				.desc("Base URL for the generated TPC-DS tables data")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("results-base-url")
                .desc("Base URL for the results and saved queries")
                .hasArg(true).required(false).build() )

        options.addOption( Option.builder().longOpt("gen-data-tag")
				.desc("Unix timestamp identifying the generated data")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("run-exp-tag")
				.desc("Unix timestamp identifying the experiment")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("table-format")
				.desc("File format to use for the tables")
				.hasArg(true).required(false).build() )
        options.addOption( Option.builder().longOpt("output-sql")
				.desc("Output the benchmark SQL statements")
				.hasArg(true).required(false).build() )
        
    }

    def getOptions() : Options = {
        return options
    } 

}
