library("rio")

#######################################################
testsToDo <- c("load", "power", "tput")
numNodes<-new.env()
numNodes[["prestoemr"]]<-9
numNodes[["sparkdatabricks"]]<-9
nodeCostPerHour<-new.env()
#EC2 cost + EMR cost (https://aws.amazon.com/emr/pricing/)
nodeCostPerHour[["prestoemr"]]<-sum(0.624, 0.156)
#2 DBUs
nodeCostPerHour[["sparkdatabricks"]]<-1.1
#######################################################

for(testDone in testsToDo) {
  doTest(testDone)
}

#Merge the instances of the searchedFile (must be .xlsx files) found in the
#workDir, and generate an output xlsx file as specified by outFile. 
#It is assumed that the instances of the searchedFile are inside each of the
#subdirectories of the workDir.
mergeXLSXFiles <- function(workDir, searchedFile, outFile) {
  subDirs <- list.files(path=workDir)
  outputDF <- NULL
  firstFile <- TRUE
  for(subDir in subDirs) {
    #Skip files, only consider folders.
    if( file_test("-f", paste0(workDir, "/", subDir)) )
      next
    inFile <- paste(workDir, "/", subDir, "/", searchedFile, sep="")
    if( firstFile ) {
      outputDF <- import(inFile)
    }
    else {
      tempDF <- import(inFile)
      outputDF[nrow(outputDF) + 1,] = tempDF[1,]
    }
    firstFile <- FALSE
  }
  outputDF$COST[outputDF$SYSTEM == "prestoemr"] <- numNodes[["prestoemr"]] * nodeCostPerHour[["prestoemr"]] * outputDF$TOTAL_DURATION_HOUR[outputDF$SYSTEM == "prestoemr"]
  outputDF$COST[outputDF$SYSTEM == "sparkdatabricks"] <- numNodes[["sparkdatabricks"]] * nodeCostPerHour[["sparkdatabricks"]] * outputDF$TOTAL_DURATION_HOUR[outputDF$SYSTEM == "sparkdatabricks"]         
  print(outputDF)
  export(outputDF, outFile)
}

doTest <- function(testDone) {
  print(paste0("Processing files for the ", testDone, " test."))
  destDir <- paste0("./Documents/RESULTS/", testDone)
  outFileName <- paste0("summary_", testDone, ".xlsx")
  outFile <- paste0(destDir, "/", outFileName, sep="")
  workDir <- paste0("./Documents/RESULTS/", testDone)
  searchedFile <- "summary.xlsx"
  mergeXLSXFiles(workDir, searchedFile, outFile)
}












