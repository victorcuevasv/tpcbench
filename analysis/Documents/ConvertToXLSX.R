library("rio")

workDir <- "./Documents/RESULTS/power"
systemDirs <- list.files(path=workDir)

#It is assumed that there is a single .log file with the run data.
for(system in systemDirs) {
  systemDir <- paste(workDir, "/", system, sep="")
  files <- list.files(path=systemDir, pattern = "\\.log$")
  inFile <- paste(workDir, "/", system, "/", files[1], sep="")
  outFileName <- paste(tools::file_path_sans_ext(files[1]), ".xlsx", sep="")
  outFile <- paste(workDir, "/", system, "/", outFileName, sep="")
  #Add colClasses="character" to read the file as characters.
  #Otherwise, long numbers will not be read correctly.
  analytics <- import(inFile, format="psv", colClasses="character")
  #Convert the character colums to numeric values where needed.
  analytics$QUERY <- as.numeric(analytics$QUERY)
  analytics$STARTDATE_EPOCH <- as.numeric(analytics$STARTDATE_EPOCH)
  analytics$STOPDATE_EPOCH <- as.numeric(analytics$STOPDATE_EPOCH)
  analytics$DURATION_MS <- as.numeric(analytics$DURATION_MS)
  analytics$DURATION <- as.numeric(analytics$DURATION)
  analytics$RESULTS_SIZE <- as.numeric(analytics$RESULTS_SIZE)
  export(analytics, outFile)
}


