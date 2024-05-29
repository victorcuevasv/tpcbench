import java.text.SimpleDateFormat
import java.util.Date
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.nio.file.Paths

class AnalyticsRecorder(val resultsDir: String,
                        val test: String,
                        val instance: Int,
                        val system: String) {

  var writer: BufferedWriter = null
  var logFile: File = null
  
  def createWriter(): Unit = {
    try {
      this.logFile = new File(
        Paths.get(this.resultsDir, this.test, "analytics",
          this.instance.toString, "analytics.csv").toString
      )
      logFile.getParentFile.mkdirs()
      this.writer = new BufferedWriter(new FileWriter(logFile))
    }
    catch {
       case e: Exception => {
        e.printStackTrace()
      }
    }
  } 

  def message(msg: String): Unit = {
   try {
        this.writer.write(msg)
        println(msg)
        this.writer.newLine()
        this.writer.flush()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
    
  def header(): Unit = {
    val titles: Array[String] = Array("QUERY",
                                      "RUN",
                                      "SUCCESSFUL",
                                      "DURATION",
                                      "RESULTS_SIZE",
                                      "SYSTEM",
                                      "STARTDATE_EPOCH",
                                      "STOPDATE_EPOCH",
                                      "DURATION_MS",
                                      "STARTDATE",
                                      "STOPDATE",
                                      "TUPLES")
    val builder: StringBuilder = new StringBuilder()
    for (i <- 0 until titles.length - 1)
      builder.append(String.format("%-25s|", titles(i)))
    builder.append(String.format("%-25s", titles(titles.length - 1)))
    this.message(builder.toString)
  }
  
  
  def record(queryRecord: QueryRecord): Unit = {
    val spaces: Int = 25
    val colFormat: String = "%-" + spaces + "s|"
    val builder: StringBuilder = new StringBuilder()
    val formatQuery = String.format(colFormat, queryRecord.query.toString)
    builder.append(formatQuery)
    val formatRun = String.format(colFormat, queryRecord.run.toString)
    builder.append(formatRun)
    val formatSuccessful = String.format(colFormat, queryRecord.successful.toString)
    builder.append(formatSuccessful)
    val durationMs: Long = queryRecord.endTime - queryRecord.startTime
    val durationFormatted: String =
      String.format("%.3f", new java.lang.Double(durationMs / 1000.0d))
    val formatDuration = String.format(colFormat, durationFormatted.toString)
    builder.append(formatDuration)
    val formatResults = String.format(colFormat, queryRecord.resultsSize.toString)
    builder.append(formatResults)
    val formatSystem = String.format(colFormat, this.system)
    builder.append(formatSystem)
    val formatStart = String.format(colFormat, queryRecord.startTime.toString)
    builder.append(formatStart)
    val formatEnd = String.format(colFormat, queryRecord.endTime.toString)
    builder.append(formatEnd)
    val formatDurationMs = String.format(colFormat, durationMs.toString)
    builder.append(formatDurationMs)
    val startDate: Date = new Date(queryRecord.startTime)
    val startDateFormatted: String =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(startDate)
    builder.append(String.format(colFormat, startDateFormatted))
    val endDate: Date = new Date(queryRecord.endTime)
    val endDateFormatted: String =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endDate)
    val formatEndDate = String.format(colFormat, endDateFormatted.toString)
    builder.append(formatEndDate)
    val endColFormat: String = "%-" + spaces + "s"
    val formatTuples = String.format(endColFormat, queryRecord.tuples.toString)
    builder.append(formatTuples)
    this.message(builder.toString)
  }

  def close(): Unit = {
    try this.writer.close()
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def getLogFileCanonicalPath(): String = {
    this.logFile.getCanonicalPath()
  }

  def getLogFileConstructorPath(): String = {
    this.logFile.getPath()
  }
  
}

