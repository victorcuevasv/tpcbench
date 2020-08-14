package org.bsc.dcc.vcv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;


public class HudiUtil {
	
	
	private final String dbName;
	private final String workDir;
	private final String resultsDir;
	private final String experimentName;
	private final int instance;
	
	
	public HudiUtil(String dbName, String workDir, String resultsDir, String experimentName, int instance) {
		this.dbName = dbName;
		this.workDir = workDir;
		this.resultsDir = resultsDir;
		this.experimentName = experimentName;
		this.instance = instance;
	}
	
	
	public Map<String, String> createHudiOptions(String tableName, String primaryKey,
			String precombineKey, String partitionKey, boolean usePartitioning) {
		Map<String, String> map = new HashMap<String, String>();
		//Use only simple keys.
		//StringTokenizer tokenizer = new StringTokenizer(primaryKey, ",");
		//primaryKey = tokenizer.nextToken();
		map.put("hoodie.datasource.hive_sync.database", this.dbName);
		map.put("hoodie.datasource.write.precombine.field", precombineKey);
		map.put("hoodie.datasource.hive_sync.table", tableName);
		map.put("hoodie.datasource.hive_sync.enable", "true");
		map.put("hoodie.datasource.write.recordkey.field", primaryKey);
		map.put("hoodie.table.name", tableName);
		//map.put("hoodie.datasource.write.storage.type", "COPY_ON_WRITE");
		map.put("hoodie.datasource.write.storage.type", "MERGE_ON_READ");
		map.put("hoodie.datasource.write.hive_style_partitioning", "true");
		map.put("hoodie.parquet.max.file.size", String.valueOf(1024 * 1024 * 1024));
		map.put("hoodie.parquet.compression.codec", "snappy");
		if( usePartitioning ) {
			map.put("hoodie.datasource.hive_sync.partition_extractor_class", 
					"org.apache.hudi.hive.MultiPartKeysValueExtractor");
			map.put("hoodie.datasource.hive_sync.partition_fields", partitionKey);
			map.put("hoodie.datasource.write.partitionpath.field", partitionKey);
			map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");
			//map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
		}
		else {
			map.put("hoodie.datasource.hive_sync.partition_extractor_class", 
					"org.apache.hudi.hive.NonPartitionedExtractor");
			map.put("hoodie.datasource.hive_sync.partition_fields", "");
			map.put("hoodie.datasource.write.partitionpath.field", "");
			map.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator");   
		}
		return map;
	}
	
	
	public void saveHudiOptions(String suffix, String tableName, Map<String, String> map) {
		try {
			String createTableFileName = this.workDir + "/" + this.resultsDir + "/" + "tables" +
					suffix + "/" + this.experimentName + "/" + this.instance +
					"/" + tableName + ".txt";
			StringBuilder builder = new StringBuilder();
			for (Map.Entry<String, String> entry : map.entrySet()) {
			    builder.append(entry.getKey() + "=" + entry.getValue().toString() + "\n");
			}
			File temp = new File(createTableFileName);
			temp.getParentFile().mkdirs();
			FileWriter fileWriter = new FileWriter(createTableFileName);
			PrintWriter printWriter = new PrintWriter(fileWriter);
			printWriter.println(builder.toString());
			printWriter.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	
}


