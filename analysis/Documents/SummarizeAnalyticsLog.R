library("rio")

#######################################################
testsToDo <- c("load", "power", "tput")
scaleFactor <- 1000
#######################################################

for(testDone in testsToDo) {
  doTest(testDone)
}

summarizeLog <- function(inFile, outFile, scaleFactor, testDone) {
  #Add colClasses="character" to read the file as characters.
  #Otherwise, long numbers will not be read correctly.
  analytics <- import(inFile, format="psv", colClasses="character")
  #Convert the character colums to numeric values where needed.
  analytics$STARTDATE_EPOCH <- as.numeric(analytics$STARTDATE_EPOCH)
  analytics$STOPDATE_EPOCH <- as.numeric(analytics$STOPDATE_EPOCH)
  analytics$DURATION_MS <- as.numeric(analytics$DURATION_MS)
  analytics$DURATION <- as.numeric(analytics$DURATION)
  totalDurationMSEC <- max(analytics$STOPDATE_EPOCH) - min(analytics$STARTDATE_EPOCH)
  totalDurationSEC <- totalDurationMSEC / 1000
  totalDurationHOUR <- totalDurationSEC / 3600
  avgDurationSEC = mean(analytics$DURATION_MS) / 1000
  geoMeanDurationSEC = prod(analytics$DURATION)^(1/length(analytics$DURATION))
  #Get the system from the first row
  loggedSystem <- analytics$SYSTEM[1]
  #Create a new dataframe to hold the aggregate results.
  outputDF <- data.frame(SYSTEM=character(),
                         TEST=character(),
                         SCALE_FACTOR=integer(),
                         TOTAL_DURATION_SEC=double(),
                         TOTAL_DURATION_HOUR=double(),
                         AVG_QUERY_DURATION_SEC=double(),
                         GEOM_MEAN_QUERY_DURATION_SEC=double(),
                         stringsAsFactors=FALSE)
  outputDF[nrow(outputDF) + 1,] = list(loggedSystem, testDone, scaleFactor, totalDurationSEC, 
                                       totalDurationHOUR, avgDurationSEC, geoMeanDurationSEC)
  export(outputDF, outFile)
}

doTest <- function(testDone) {
  print(paste0("Processing files for the ", testDone, " test."))
  workDir <- paste0("./Documents/RESULTS/", testDone)
  systemDirs <- list.files(path=workDir)
  for(system in systemDirs) {
    #Skip files and only process directories
    if( file_test("-f", paste0(workDir, "/", system)) ) {
      next
    }
    systemDir <- paste(workDir, "/", system, sep="")
    files <- list.files(path=systemDir, pattern = "\\.log$")
    inFile <- paste(workDir, "/", system, "/", files[1], sep="")
    outFileName <- "summary.xlsx"
    outFile <- paste(workDir, "/", system, "/", outFileName, sep="")
    summarizeLog(inFile, outFile, scaleFactor, testDone)
  }
}










