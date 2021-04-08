package org.bsc.dcc.vcv;

import org.apache.spark.sql.SparkSession;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.actions.Actions;

public class IcebergUtil {
	
	public IcebergUtil() {
		
	}
	
	public void rewriteData(SparkSession spark, String dbName, String tableName, int sizeInMB) {
		TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
		HiveCatalog catalog = new HiveCatalog(spark.sparkContext().hadoopConfiguration());
		Table table = catalog.loadTable(tableId);
		//Rewrite with a size of 100 mb
		Actions.forTable(table).rewriteDataFiles().targetSizeInBytes(sizeInMB * 1024 * 1024).execute();
	}

}


