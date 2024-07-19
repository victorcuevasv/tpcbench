import org.apache.spark.sql.SparkSession
import java.time.Instant
import java.net.URL
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.spark.sql.Encoders
import scala.collection.JavaConverters._

object TpcdsBench extends App {

  var spark: SparkSession = SparkSession.builder().appName("TPC-DS Benchmark").enableHiveSupport().getOrCreate()

  val cmdLine : CommandLine = parseArgs(args)
  if (cmdLine == null) {
    System.out.println("Error parsing the command line options")
    System.exit(1)
  } 

  val timestamp = Instant.now.getEpochSecond
  // Flags for the execution of different tests create_db|load|power
  var flags = cmdLine.getOptionValue("exec-flags")
  flags = if (flags == null) "111" else flags
  // Scale factor in GB for the benchmark
  var scaleFactorStr = cmdLine.getOptionValue("scale-factor")
  scaleFactorStr = if (scaleFactorStr == null) "1" else scaleFactorStr
  val scaleFactor = scaleFactorStr.toInt
  // URL of the input raw TPC-DS CSV data
  var rawBaseURL = cmdLine.getOptionValue("raw-base-url")
  rawBaseURL = if (rawBaseURL == null) "s3://tpcds-data-1713123644/" else rawBaseURL
  // Base URL for the generated TPC-DS tables data
  var warehouseBaseURL = cmdLine.getOptionValue("warehouse-base-url")
  warehouseBaseURL = if (warehouseBaseURL == null) "s3://tpcds-warehouses-1713123644/" else warehouseBaseURL
  // Base URL for the results and saved queries
  var resultsBaseURL = cmdLine.getOptionValue("results-base-url")
  resultsBaseURL = if (resultsBaseURL == null) "s3://tpcds-results-1713123644/" else resultsBaseURL
 
  // Unix timestamp identifying the generated data
  var genDataTagStr = cmdLine.getOptionValue("gen-data-tag")
  genDataTagStr = if (genDataTagStr == null) timestamp.toString else genDataTagStr
  val genDataTag = genDataTagStr.toInt
  // Unix timestamp identifying the experiment
  var runExpTagStr = cmdLine.getOptionValue("run-exp-tag")
  runExpTagStr = if (runExpTagStr == null) timestamp.toString else runExpTagStr
  val runExpTag = runExpTagStr.toInt
  // File format to use for the tables
  var tableFormat = cmdLine.getOptionValue("table-format")
  tableFormat = if (tableFormat == null) "parquet" else tableFormat
  // Output the benchmark SQL statements
  var isOutputSqlStr = cmdLine.getOptionValue("is-output-sql")
  isOutputSqlStr = if (isOutputSqlStr == null) "true" else isOutputSqlStr
  val isOutputSql = isOutputSqlStr.toBoolean

  val dbName = s"tpcds_sf${scaleFactor}_${genDataTag}"
  val rawLocation = TpcdsBenchUtil.addPathToURI(rawBaseURL, s"${scaleFactor}GB")
  val whLocation = TpcdsBenchUtil.addPathToURI(warehouseBaseURL, dbName)
  val expName = s"${dbName}_${runExpTag}"
  val resultsLocation = TpcdsBenchUtil.addPathToURI(resultsBaseURL, expName)
  val resultsDir = expName
  val system = "spark"

  //Fact Tables partition keys
  val partitionKeys = Map (
    "catalog_returns" -> "cr_returned_date_sk",
    "catalog_sales" -> "cs_sold_date_sk",
    "inventory" -> "inv_date_sk",
    "store_returns" -> "sr_returned_date_sk",
    "store_sales" -> "ss_sold_date_sk",
    "web_returns" -> "wr_returned_date_sk",
    "web_sales" -> "ws_sold_date_sk"
  )

  runTests(dbName, flags, scaleFactor, rawLocation, whLocation, resultsLocation, partitionKeys, tableFormat,
      resultsDir, system)

  def parseArgs(args: Array[String]) : CommandLine = {
		var cmdLine : CommandLine = null
    try {
			val opts = new RunBenchmarkOptions()
      opts.initOptions()
			val parser: CommandLineParser = new DefaultParser()
			cmdLine = parser.parse(opts.getOptions(), args)
		}
    catch {
      case e: Exception => {  
        println(e.getMessage)
      }
    }
    return cmdLine
  }

  def sqlStmt(stmt: String, descShort: String, descLong: String) : Unit = {
    spark.sparkContext.setLocalProperty("callSite.short", descShort)
    spark.sparkContext.setLocalProperty("callSite.long", descLong)
    sqlStmt(stmt)
  }

  def sqlStmt(stmt: String) : Unit = { 
    if( isOutputSql )
      println(stmt)
    try {
      spark.sql(stmt)
    }
    catch {
      case e: Exception => {  
        println(e.getMessage)
      }
    }
  }

  def runTests(dbName: String, flags: String, scaleFactor: Integer, rawLocation: String, whLocation: String,
    resultsLocation: String, partitionKeys: Map[String, String], tableFormat: String, resultsDir: String,
    system: String) = {
    println(s"Running the TPC-DS benchmark at the ${scaleFactor} scale factor.")
    if ( flags.charAt(0) == '1' )
      createDatabase(dbName)
    if ( flags.charAt(1) == '1')
      runLoadTest("load", 1, dbName, scaleFactor, rawLocation, whLocation, resultsLocation, partitionKeys,
        tableFormat, resultsDir, system)
    if ( flags.charAt(2) == '1')
      runPowerTest("power", dbName, resultsDir, resultsLocation, scaleFactor, 1, system) 
  }

  def createDatabase(dbName: String) = {
    println(s"Creating database ${dbName}.")
    sqlStmt(s"DROP DATABASE IF EXISTS ${dbName}")
    sqlStmt(s"CREATE DATABASE IF NOT EXISTS ${dbName}")
  }

  def useDatabase(dbName: String) = {
      sqlStmt(s"USE ${dbName}")
  }

  def runLoadTest(testName: String, instance: Integer, dbName: String, scaleFactor: Integer, rawLocation: String,
    whLocation: String, resultsLocation: String, partitionKeys: Map[String, String], tableFormat: String,
    resultsDir: String, system: String) = {
    println(s"Running the TPC-DS benchmark load test at the ${scaleFactor} scale factor.")
    useDatabase(dbName)
    val schemasMap = new TPCDS_Schemas().tpcdsSchemasMap
    var tableNames = schemasMap.keys.toList.sorted

    /////////////////////////////
    //tableNames = List("call_center", "catalog_page", "customer_address", "customer_demographics", "customer", "date_dim", 
    //"household_demographics", "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "time_dim", 
    //"warehouse", "web_page", "web_site")
    /////////////////////////////

    val recorder = new AnalyticsRecorder(resultsDir, testName, instance, system)
    recorder.createWriter()
    recorder.header()
    var query = 1
    for (tableName <- tableNames) {
      loadTable(testName, tableName, schemasMap(tableName), rawLocation, whLocation, resultsLocation, partitionKeys, tableFormat,
        resultsDir, system, query, 1, recorder)
      query += 1
    }
    recorder.close()
    TpcdsBenchUtil.uploadFileToS3(resultsLocation, 
      s"analytics/${testName}/${instance}/analytics.csv", recorder.getLogFileCanonicalPath())
  }

  def loadTable(testName: String, tableName: String, schema: String, rawLocation: String, whLocation: String,
    resultsLocation: String, partitionKeys: Map[String, String], tableFormat: String, resultsDir: String,
    system: String, query: Integer, run: Integer, recorder: AnalyticsRecorder) = {
    var successful = false
    val startTime = System.currentTimeMillis()
    try {
      println(s"START: load table $tableName")
      val createExtStmt = genExtTableStmt(tableName, schema, rawLocation)
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/external/${tableName}.sql", createExtStmt)
      sqlStmt(createExtStmt, s"create ext $tableName", s"create external table $tableName")
      val createWhStmt = genWarehouseTableStmt(tableName, schema, whLocation, partitionKeys, tableFormat)
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/warehouse/${tableName}.sql", createWhStmt)
      sqlStmt(createWhStmt, s"create $tableName", s"create table $tableName")
      val insertStmt = genInsertStmt(tableName, schema, partitionKeys)
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/insert/${tableName}.sql", insertStmt)
      sqlStmt(insertStmt, s"insert $tableName", s"insert into $tableName")
      successful = true
      println(s"END: load table $tableName" )
    }
    catch {
      case e: Exception => {  
        println(e.getMessage)
      }
    }
    finally {
      val endTime = System.currentTimeMillis()
      val record = new QueryRecord(query, run, startTime, endTime, successful, 0, 0)
      recorder.record(record)
    }
  }

  def runPowerTest(testName: String, dbName: String, resultsDir: String, resultsLocation: String, scaleFactor: Integer, 
    instance: Integer, system: String) = {
    println(s"Running the TPC-DS benchmark power test at the ${scaleFactor} scale factor.")
    useDatabase(dbName)
    val recorder = new AnalyticsRecorder(resultsDir, testName, instance, system)
    recorder.createWriter()
    recorder.header()
    val queriesMap = Class.forName(s"TPCDS_Queries${scaleFactor}GB").getDeclaredConstructor().newInstance().asInstanceOf[TPCDS_Queries].getTpcdsQueriesMap()
    val queryNums = queriesMap.keys.toList.map(_.replace("query", "")).map(_.toInt).sorted
    for (nQuery <- queryNums) {
      val queryStr = queriesMap(s"query${nQuery}")
      runQuery(testName, queryStr, resultsLocation, resultsDir, system, nQuery, 1, recorder)
    }
    recorder.close()
    TpcdsBenchUtil.uploadFileToS3(resultsLocation, 
      s"analytics/${testName}/${instance}/analytics.csv", recorder.getLogFileCanonicalPath())
  }

  def runQuery(testName: String, queryStr: String, resultsLocation: String, resultsDir: String,
    system: String, nQuery: Integer, run: Integer, recorder: AnalyticsRecorder) = {
    var successful = false
    var nTuples = 0
    var resListStr = ""
    val startTime = System.currentTimeMillis()
    try {
      println(s"START: run query $nQuery")
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/query/query${nQuery}.sql", queryStr)
      spark.sparkContext.setLocalProperty("callSite.short", s"query $nQuery")
      spark.sparkContext.setLocalProperty("callSite.long", s"query $nQuery")
      val subQueries = queryStr.split(";").map(_.trim).filter(_.length > 0)
      for( subQueryStr <- subQueries ) {
        val res = spark.sql(subQueryStr)
        val resList = res.map(_.mkString(" | "), Encoders.STRING).collectAsList
        nTuples += resList.size()
        resListStr += asScalaBuffer(resList).mkString("\n")
      }
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/result/query${nQuery}.csv", resListStr)
      successful = true
      println(s"END: run query $nQuery" )
    }
    catch {
      case e: Exception => {  
        println(e.getMessage)
      }
    }
    finally {
      val endTime = System.currentTimeMillis()
      val record = new QueryRecord(nQuery, run, startTime, endTime, successful, 0, nTuples)
      recorder.record(record)
    }
  }

  // Parse the schema of a given table out of a "create table" query generated by the tpcds toolkit.
  // Returns an array of tuples (Attribute, Type)
  def parseSchemaFromSQL(schema: String) : Array[(String, String)]= {
    schema
    .split("\n")    // Split the schema into lines
    .drop(2)        // Drop the CREATE TABLE and the parenthesis on the first two lines
    .map(           
      _.split(" +") // Split the lines by any amount of spaces generating arrays of strings (tokens)
      .drop(1)      // Drop the initial whitespace
      .take(2)      // Take only the first two elements as we do only want the column name and type
    ).filter( // Filter out empty lines and primary key lines (if any)
      tokens => tokens.length > 0 && tokens(0) != "primary")
    .map( // Generate the (attribute, type) pairs, changing integer into int when applicable
      tokens => if (tokens(1) == "integer") (tokens(0), "int") else (tokens(0), tokens(1))) 
  }

  // Generate a SQL statement to create an external table over the raw CSV data
  def genExtTableStmt(tableName: String, schema: String, whLocation: String) = {
    var sb = new StringBuilder(s"CREATE EXTERNAL TABLE ${tableName}_ext\n(\n")
    sb ++= parseSchemaFromSQL(schema).map(t=> s"    ${t._1} ${t._2}").mkString(",\n")
    sb ++= s"""
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '${whLocation}/${tableName}'
    """
    sb.toString()
  }

  // Generate a SQL statement to create a warehouse table to store the data in the desired format
  def genWarehouseTableStmt(tableName: String, schema: String, whLocation: String, 
    partitionKeys: Map[String, String], tableFormat: String) = {
    var sb = new StringBuilder(s"CREATE TABLE ${tableName}\n(\n")
    sb ++= parseSchemaFromSQL(schema).map(t=> s"    ${t._1} ${t._2}").mkString(",\n")
    sb ++= s"""
    )
    USING ${tableFormat}
    OPTIONS('compression'='gzip')
    """
    if (partitionKeys.contains(tableName)) sb ++= s"PARTITIONED BY (${partitionKeys(tableName)})\n"
    sb ++= s"LOCATION '${whLocation}/${tableName}'"
    sb.toString()
  }

  // Generate a SQL statement to insert the data from the external table into the warehouse table
  def genInsertStmt(tableName: String, schema: String, partitionKeys: Map[String, String]) = {
    var sb = new StringBuilder(s"INSERT OVERWRITE TABLE ${tableName} SELECT")
    // If the table is partitioned, the attributes need to be listed with the partition attributes at the end
    if (partitionKeys.contains(tableName)) {
      sb ++= "\n"                                            // Add a new line if we have to list the attributes
      sb ++= parseSchemaFromSQL(schema)                   // Get the list of attributes
      .map(_._1)                                             // Select only the attribute names
      .filter(_ != partitionKeys(tableName))                 // Filter out the partition key
      .mkString(",\n")                                       // Concatenate into a string
      sb ++= s",\n${partitionKeys(tableName)}\n"             // Add the partition key to the end of the list
    } else sb ++= " * "                                      // Select everything if the table is not partitioned
    sb ++= s"FROM ${tableName}_ext"
    // If the table is partitioned add a DISTRIBUTE BY directive
    if (partitionKeys.contains(tableName)) sb ++= s"\nDISTRIBUTE BY ${partitionKeys(tableName)}"
    sb.toString()
  }
  

}

