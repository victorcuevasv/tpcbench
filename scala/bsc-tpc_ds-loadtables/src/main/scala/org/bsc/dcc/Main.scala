package org.bsc.dcc

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class TPCDSLoadTables(val spark : SparkSession) {
  
  //Auxiliary functions
    
  // Runs and prints the start and end banners for each SQL statement
  def runQuery(str: String) = {
    val banner = str.split("\n")(0) + "..."
    println(s"START: $banner")  
    try {
      spark.sql(str)
      println(s"END: $banner" )
    } catch {
      case e: Exception => {  
        println(s"ERROR: $banner")
        println(e.getMessage)
      }
    }
  }
  
  // Parse the schema of a given table out of a "create table" query generated by the tpcds toolkit. Used to build the insertion queries.
  // Returns an array of tuples (Attribute, Type)
  def parseSchemaFromSQL(createTableDict: Map[String, String], tableName: String) = {
    createTableDict(tableName)
    .split("\n")    // Split the schema into lines
    .drop(2)        // Drop the CREATE TABLE and the parentheses on the first two lines
    .map(           
      _.split(" +") // Split the line by any amount of spaces
      .drop(1)      // Drop the initial whitespace
      .take(2)      // Take only the first two as we do only want the column name and type
    ).filter(tokens => tokens.length > 0 && tokens(0) != "primary")  // Filter out empty lines and primary key lines (if any)
    .map(tokens => if (tokens(1) == "integer") (tokens(0), "int") else (tokens(0), tokens(1)))
  }
  
  //Load external textfile table from S3
  // Generate the query to create an external textfile table
  def genTextfileQuery(createTableDict: Map[String, String], tableName: String, sourceLocation: String) = {
    var sb = new StringBuilder(s"CREATE EXTERNAL TABLE ${tableName}_ext\n(\n")
    sb ++= parseSchemaFromSQL(createTableDict, tableName).map(t=> s"    ${t._1} ${t._2}").mkString(",\n")
    sb ++= s"""
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '${sourceLocation}/${tableName}'
    """
    sb.toString()
  }
  
  def createTextfileTable(createTableDict: Map[String, String], tableName: String, sourceLocation: String) : String = {
    val textFileQuery = genTextfileQuery(createTableDict, tableName, sourceLocation)
    runQuery(textFileQuery)
    textFileQuery
  }
  
  //Create and populate parquet tables from textfile tables
  // Generate the query to create the empty parquet table
  def genCreateParquetQuery(createTableDict: Map[String, String], tableName: String, targetLocation: String, partitionKeys: Map[String, String]) = {
    var sb = new StringBuilder(s"CREATE TABLE ${tableName}\n(\n")
    sb ++= parseSchemaFromSQL(createTableDict, tableName).map(t=> s"    ${t._1} ${t._2}").mkString(",\n")
    sb ++= s"""
    )
    USING PARQUET
    OPTIONS('compression'='snappy')
    """
    if (partitionKeys.contains(tableName)) sb ++= s"PARTITIONED BY (${partitionKeys(tableName)})\n"
    sb ++= s"LOCATION '${targetLocation}/${tableName}'"
    sb.toString()
  }

  // Generate the query to insert the data from the textfile external table into the parquet table
  def genInsertParquetQuery(createTableDict: Map[String, String], tableName: String, partitionKeys: Map[String, String]) = {
    var sb = new StringBuilder(s"INSERT OVERWRITE TABLE ${tableName} SELECT")
    if (partitionKeys.contains(tableName)) {
      sb ++= "\n"                                            // Add a new line if we have to list the attributes
      sb ++= parseSchemaFromSQL(createTableDict, tableName)  // Get the list of attributes
      .map(_._1)                                             // Select only the attribute names
      .filter(_ != partitionKeys(tableName))                 // Filter out the partition key
      .mkString(",\n")                                       // Concatenate into a string
      sb ++= s",\n${partitionKeys(tableName)}\n"             // Add the partition key to the end of the select
    } else sb ++= " * "                                      // Select everything if the table is not partitioned
    sb ++= s"FROM ${tableName}_ext"
    // If the table is partitioned add a DISTRIBUTE BY directive
    if (partitionKeys.contains(tableName)) sb ++= s"\nDISTRIBUTE BY ${partitionKeys(tableName)}"
    sb.toString()
  }
  
  def createParquetTable(createTableDict: Map[String, String], tableName: String, targetLocation: String, partitionKeys: Map[String, String]) :
    (String, String) = {
    val createParquetQuery = genCreateParquetQuery(createTableDict, tableName, targetLocation, partitionKeys)
    runQuery(createParquetQuery)
    val insertParquetQuery = genInsertParquetQuery(createTableDict, tableName, partitionKeys)
    runQuery(insertParquetQuery)
    (createParquetQuery, insertParquetQuery)
  }
  
  //Create the denormalized tables from the parquet tables
  // Generate the query to denormalize one of the selected fact tables listed in denormQueries
  def genDenormQuery(tableName: String, targetLocation: String, partitionKeys: Map[String, String], denormQueries: Map[String, String]) = {
    var sb = new StringBuilder(
      s"""CREATE TABLE ${tableName}_denorm
      USING PARQUET
      OPTIONS('compression'='snappy')
      LOCATION '${targetLocation}/${tableName}_denorm'
      PARTITIONED BY (${partitionKeys(tableName)}) AS
      """
    )
    sb ++= denormQueries(tableName)
    sb ++= s"\nDISTRIBUTE BY ${partitionKeys(tableName)}"
    sb.toString()
  }
  
  def createDenormTable(tableName: String, targetLocation: String, partitionKeys: Map[String, String], denormQueries: Map[String, String]) = {
    val denormQuery = genDenormQuery(tableName, targetLocation, partitionKeys, denormQueries)
    runQuery(denormQuery)
    denormQuery
  }
  
  //Generate the data to be inserted into Delta/Hudi tables by sampling the denormalized table
  // Generate the query to sample the denormalized table in order to generate the data to be inserted into Delta/Hudi
  def genSkipQuery(tableName: String, partitionKeys: Map[String, String], skipAttribute: String, skipMod: Int, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(
      s"""SELECT * FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},2) <> 0
      OR MOD(${skipAttribute},${skipMod}) <> 0
      """
    )
    if (partitionThreshold > 0) sb ++= s"\nOR ${partitionKeys(tableName)} <= ${partitionThreshold}"
    sb.toString()
  }
  
  def createSkipTable(tableName: String, targetLocation: String, partitionKeys: Map[String, String], skipAttr: String, skipMod: Int,
    partitionThreshold: Int = -1) = {
    val skipQuery = genSkipQuery(tableName, partitionKeys, skipAttr, skipMod, partitionThreshold)
    spark.sql(skipQuery).write
      .option("path", s"${targetLocation}/${tableName}_denorm_skip")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_skip")
    skipQuery
  }

  //Generate the data to be Upserted into Delta/Hudi tables by sampling the denormalized table
  def genInsertDataQuery(tableName: String, partitionKeys: Map[String, String], skipAttr: String, skipMod: Int, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(
      s"""SELECT * FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},2) = 0
      AND MOD(${skipAttr},${skipMod}) = 0
      """
    )
    if (partitionThreshold > 0) sb ++= s"\nAND ${partitionKeys(tableName)} > ${partitionThreshold}"
    sb.toString()
  }
  
  def genUpdateDataQuery(createTableDict: Map[String, String], tableName: String, partitionKeys: Map[String, String], skipAttr: String, skipMod: Int, 
    updateAttr: String, partitionThreshold: Int = -1) = {
    //val tableAttributes = table("store_sales_denorm").columns
    val tableAttributes = spark.sql("describe store_sales_denorm").collect.dropRight(3).map(x => x.getString(0))
    if (!(tableAttributes contains updateAttr)) {
      println(s"Error[genUpdateDataQuery]: Update attribute is not one of ${tableName}'s attributes or it is the partition key")
      None
    }
    var sb = new StringBuilder("SELECT\n")
    sb ++= tableAttributes.map(attr => if (attr == updateAttr) s"${updateAttr} + 1" else attr).mkString(",\n")
    sb ++= s"""\n${partitionKeys(tableName)}
      FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},2)=1
      AND MOD(${skipAttr},${skipMod}) = 0
      """
    if (partitionThreshold > 0) sb ++= s"\nAND ${partitionKeys(tableName)} > ${partitionThreshold}"
    sb.toString()
  }
  
  def genUpsertDataQuery(createTableDict: Map[String, String], tableName: String, partitionKeys: Map[String, String], skipAttr: String,
    skipModInsert: Int, skipModUpdate: Int, updateAttr: String, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(genInsertDataQuery(tableName, partitionKeys, skipAttr, skipModInsert, partitionThreshold))
    sb ++= "\nUNION ALL\n"
    sb ++= genUpdateDataQuery(createTableDict, tableName, partitionKeys, skipAttr, skipModUpdate, updateAttr, partitionThreshold)
    sb.toString()
  }
  
  def createUpsertTable(createTableDict: Map[String, String], tableName: String, targetLocation: String, partitionKeys: Map[String, String],
    skipAttr: String, skipModInsert: Int, skipModUpdate: Int, updateAttr: String, partitionThreshold: Int = -1) = {
    val upsertDataQuery = genUpsertDataQuery(createTableDict, tableName, partitionKeys, skipAttr, skipModInsert, skipModUpdate,
      updateAttr, partitionThreshold)
    spark.sql(upsertDataQuery)
      .write
      .option("path", s"${targetLocation}/${tableName}_denorm_upsert")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_upsert")
    upsertDataQuery
  }

  def createInsertTable(createTableDict: Map[String, String], tableName: String, targetLocation: String, partitionKeys: Map[String, String],
    skipAttr: String, skipModInsert: Int, skipModUpdate: Int, updateAttr: String, partitionThreshold: Int = -1) = {
    val insertDataQuery = genInsertDataQuery(tableName, partitionKeys, skipAttr, skipModInsert, partitionThreshold)
    spark.sql(insertDataQuery)
      .write
      .option("path", s"${targetLocation}/${tableName}_denorm_insert")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_insert")
    insertDataQuery
  }

  def genDelete01Query(tableName: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(
      s"""SELECT * FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},10) = 1 AND MOD(${skipAttr},100) = 0
      """
    )
    if (partitionThreshold > 0) sb ++= s"AND ${partitionKeys(tableName)} > ${partitionThreshold}"
    sb.toString()
  }

  def genDelete1Query(tableName: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(
      s"""SELECT * FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},10) = 0 AND MOD(${skipAttr},10) = 0
      """
    )
    if (partitionThreshold > 0) sb ++= s"AND ${partitionKeys(tableName)} > ${partitionThreshold}"
    sb.toString()
  }

  def genDelete11Query(tableName: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    var sb = new StringBuilder(
      s"""SELECT * FROM ${tableName}_denorm
      WHERE MOD(${partitionKeys(tableName)},3) = 0 AND MOD(${skipAttr},3) = 0
      """
    )
    if (partitionThreshold > 0) sb ++= s"AND ${partitionKeys(tableName)} > ${partitionThreshold}"
    sb.toString()
  }
  
  def createDelete01Table(tableName: String, targetLocation: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    val delete01Query = genDelete01Query(tableName, partitionKeys, skipAttr, partitionThreshold)
    spark.sql(delete01Query)
      .write
      .option("path", s"${targetLocation}/${tableName}_denorm_delete_01")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_delete_01")
    delete01Query
  }
  
  def createDelete1Table(tableName: String, targetLocation: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    val delete1Query = genDelete1Query(tableName, partitionKeys, skipAttr, partitionThreshold)
    spark.sql(delete1Query)
      .write
      .option("path", s"${targetLocation}/${tableName}_denorm_delete_1")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_delete_1")
    delete1Query
  }
  
  def createDelete11Table(tableName: String, targetLocation: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    val delete11Query = genDelete11Query(tableName, partitionKeys, skipAttr, partitionThreshold)
    spark.sql(delete11Query)
      .write
      .option("path", s"${targetLocation}/${tableName}_denorm_delete_11")
      .partitionBy(partitionKeys(tableName))
      .mode("overwrite").format("parquet")
      .saveAsTable(s"${tableName}_denorm_delete_11")
    delete11Query
  }
  
  def createDeleteTables(tableName: String, targetLocation: String, partitionKeys: Map[String, String], skipAttr: String, partitionThreshold: Int = -1) = {
    createDelete01Table(tableName, targetLocation, partitionKeys, skipAttr, partitionThreshold)
    createDelete1Table(tableName, targetLocation, partitionKeys, skipAttr, partitionThreshold)
    createDelete11Table(tableName, targetLocation, partitionKeys, skipAttr, partitionThreshold)
  }
  
}

object Main extends App {
  println("Hello, World!")
}
