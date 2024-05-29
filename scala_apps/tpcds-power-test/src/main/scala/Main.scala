import org.apache.spark.sql.SparkSession
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}
import java.util.concurrent.Callable
import java.time.Instant
import java.net.URL

@Command(name = "tpcdsbench", mixinStandardHelpOptions = true, version = Array("tpcdsbench 1.0"),
  description = Array("Executes the TPC-DS benchmark power test"))
class TpcdsBench extends Callable[Int] {

  val timestamp = Instant.now.getEpochSecond
  @Option(names = Array("--exec-flags"), description = Array("Flags for the execution of different tests create_db|load|power"))
  private var flags = "111"
  @Option(names = Array("--cold-run"), description = Array("Run without executing any SQL statements"))
  private var isColdRun = false
  @Option(names = Array("--scale-factor"), description = Array("Scale factor in GB for the benchmark"))
  private var scaleFactor = 1
  @Option(names = Array("--raw-base-url"), description = Array("URL of the input raw TPC-DS CSV data"))
  private var rawBaseURL = s"s3://tpcds-data-1713123644/"
  @Option(names = Array("--warehouse-base-url"), description = Array("Base URL for the generated TPC-DS tables data"))
  private var warehouseBaseURL = s"s3://tpcds-warehouses-1713123644/"
  @Option(names = Array("--results-base-url"), description = Array("Base URL for the results and saved queries"))
  private var resultsBaseURL = s"s3://tpcds-results-1713123644/"
  @Option(names = Array("--gen-data-tag"), description = Array("Unix timestamp identifying the generated data"))
  var genDataTag = timestamp
  @Option(names = Array("--run-exp-tag"), description = Array("Unix timestamp identifying the experiment"))
  var runExpTag = timestamp
  @Option(names = Array("--table-format"), description = Array("File format to use for the tables"))
  var tableFormat = "parquet"
  @Option(names = Array("--output-sql"), description = Array("Output the benchmark SQL statements"))
  private var isOutputSql = true

  def sqlStmtOut(stmt: String) = { 
    if( isOutputSql )
      println(stmt)
  }

  def sqlStmtSpark(stmt: String) = { 
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

  var spark: SparkSession = SparkSession.builder().appName("TPC-DS Benchmark").enableHiveSupport().getOrCreate()

  var sqlStmt = sqlStmtOut(_)
  if( ! isColdRun )
    sqlStmt = sqlStmtSpark(_)

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

  def call(): Int = {
    runTests(dbName, flags, scaleFactor, rawLocation, whLocation, resultsLocation, partitionKeys, tableFormat,
      resultsDir, system)
    0
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
      runPowerTest()
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
    val tableNames = schemasMap.keys.toList.sorted
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
      sqlStmt(createExtStmt)
      val createWhStmt = genWarehouseTableStmt(tableName, schema, whLocation, partitionKeys, tableFormat)
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/warehouse/${tableName}.sql", createWhStmt)
      sqlStmt(createWhStmt)
      val insertStmt = genInsertStmt(tableName, schema, partitionKeys)
      TpcdsBenchUtil.saveStringToS3(resultsLocation, s"${testName}/insert/${tableName}.sql", insertStmt)
      sqlStmt(insertStmt)
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

  def runPowerTest() = {

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
    OPTIONS('compression'='lzo')
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

object TpcdsBench {
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new TpcdsBench()).execute(args: _*))
  }
}
