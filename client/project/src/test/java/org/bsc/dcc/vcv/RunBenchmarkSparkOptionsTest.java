package org.bsc.dcc.vcv;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingOptionException;

/**
 * Unit test for simple App.
 */
public class RunBenchmarkSparkOptionsTest {

    /**
     * Rigourous Test :-)
     */
    @Test
    public void testApp() {
        assertTrue( true );
    }
    
    @Test
    public void testEmptyArguments() {
    	String[] args = new String[] {};
    	RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
    	Options options = runOptions.getOptions();
    	CommandLineParser parser = new DefaultParser();
    	MissingOptionException e = assertThrows(
    			MissingOptionException.class, () -> parser.parse(options, args)
    	);
    	assertTrue(e.getMessage().contains("Missing required options"));
    }
    
    @Test
    public void testRequiredArguments() {
    	String[] args = new String[] {
    			"--main-work-dir=/data", "--schema-name=tpcds_sparkemr_529_1gb_1_db", 
    			"--results-dir=1odwczxc3jftmhmvahdl7tz32dyyw0pen",
    			"--experiment-name=sparkemr-529-2nodes-1gb-experimental", 
    			"--system-name=sparkemr", "--instance-number=1", 
    			"--table-format=parquet", "--use-partitioning=false", 
    			"--jar-file=/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar",
    			"--use-row-stats=true", "--use-column-stats=true", "--all-or-query-file=all", 
    			"--number-of-streams=1"
    	};
    	RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
    	Options options = runOptions.getOptions();
    	CommandLineParser parser = new DefaultParser();
    	assertDoesNotThrow(() -> parser.parse(options, args));
    }
    
    @Test
    public void testBooleanArguments() {
    	String[] args = new String[] {
    			"--main-work-dir=/data", "--schema-name=tpcds_sparkemr_529_1gb_1_db", 
    			"--results-dir=1odwczxc3jftmhmvahdl7tz32dyyw0pen",
    			"--experiment-name=sparkemr-529-2nodes-1gb-experimental", 
    			"--system-name=sparkemr", "--instance-number=1", 
    			"--table-format=parquet", "--use-partitioning=false", 
    			"--jar-file=/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar",
    			"--use-row-stats=true", "--use-column-stats=true", "--all-or-query-file=all", 
    			"--number-of-streams=1"
    	};
    	try {
    		RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
    		Options options = runOptions.getOptions();
    		CommandLineParser parser = new DefaultParser();
    		CommandLine commandLine = parser.parse(options, args);
    		String partitionStr = commandLine.getOptionValue("use-partitioning");
    		boolean partition = Boolean.parseBoolean(partitionStr);
    		assertFalse(partition);
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    @Test
    public void testRecoverAsString() {
    	String[] args = new String[] {
    			"--main-work-dir=/data", "--schema-name=tpcds_sparkemr_529_1gb_1_db", 
    			"--results-dir=1odwczxc3jftmhmvahdl7tz32dyyw0pen",
    			"--experiment-name=sparkemr-529-2nodes-1gb-experimental", 
    			"--system-name=sparkemr", "--instance-number=1", 
    			"--table-format=parquet", "--use-partitioning=false", 
    			"--jar-file=/mnt/tpcds-jars/targetsparkdatabricks/client-1.2-SNAPSHOT-jar-with-dependencies.jar",
    			"--use-row-stats=true", "--use-column-stats=true", "--all-or-query-file=all", 
    			"--number-of-streams=1"
    	};
    	try {
    		RunBenchmarkSparkOptions runOptions = new RunBenchmarkSparkOptions();
    		Options options = runOptions.getOptions();
    		CommandLineParser parser = new DefaultParser();
    		CommandLine commandLine = parser.parse(options, args);
    		assertEquals(RunBenchmarkSparkOptions.recoverAsString(commandLine), String.join(",", args));
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
}

