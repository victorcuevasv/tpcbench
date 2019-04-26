library("rio")

summarizeLog <- function(inFile, outFile, scaleFactor) {
  #Add colClasses="character" to read the file as characters.
  #Otherwise, long numbers will not be read correctly.
  analytics <- import(inFile, format="psv", colClasses="character")
  #Convert the character colums to numeric values where needed.
  analytics$STARTDATE_EPOCH <- as.numeric(analytics$STARTDATE_EPOCH)
  analytics$STOPDATE_EPOCH <- as.numeric(analytics$STOPDATE_EPOCH)
  analytics$DURATION_MS <- as.numeric(analytics$DURATION_MS)
  totalDurationMSEC <- max(analytics$STOPDATE_EPOCH) - min(analytics$STARTDATE_EPOCH)
  totalDurationSEC <- totalDurationMSEC / 1000
  avgDurationSEC = mean(analytics$DURATION_MS) / 1000
  #Get the system from the first row
  loggedSystem <- analytics$SYSTEM[1]
  #Create a new dataframe to hold the aggregate results.
  outputDF <- data.frame(SYSTEM=character(),
                         SCALE_FACTOR=integer(),
                         TOTAL_DURATION=double(),
                         AVG_QUERY_DURATION=double(),
                         stringsAsFactors=FALSE)
  outputDF[nrow(outputDF) + 1,] = list(loggedSystem, scaleFactor, totalDurationSEC, avgDurationSEC)
  export(outputDF, outFile)
}

workDir <- "./Documents/RESULTS/tput"
#workDir <- "./Documents/RESULTS/power"
#workDir <- "./Documents/RESULTS/load"
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
  summarizeLog(inFile, outFile, 0)
}











