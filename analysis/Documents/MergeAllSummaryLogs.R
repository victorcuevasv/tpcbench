library("rio")

#######################################################
testsToDo <- c("load", "power", "tput")
#######################################################

destDir <- "./Documents/RESULTS/"
outFileName <- "summaryAll.xlsx"
outFile <- paste0(destDir, "/", outFileName)

firstFile <- TRUE
outputDF <- NULL

for(testDone in testsToDo) {
  print(paste0("Processing files for the ", testDone, " test."))
  workDir <- paste0("./Documents/RESULTS/", testDone)
  searchedFile <- paste0("summary_", testDone, ".xlsx")
  inFile <- paste0(workDir, "/", searchedFile)
  if( firstFile ) {
    outputDF <- import(inFile)
  }
  else {
    tempDF <- import(inFile)
    outputDF <- rbind(outputDF, tempDF)
  }
  firstFile <- FALSE
}

print(outputDF)
export(outputDF, outFile)












