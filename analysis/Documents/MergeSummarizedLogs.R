library("rio")

destDir <- "./Documents/RESULTS/power"
outFileName <- "summaryPower.xlsx"
outFile <- paste(destDir, "/", outFileName, sep="")
workDir <- "./Documents/RESULTS/power"
systemDirs <- list.files(path=workDir)
outputDF <- NULL
firstFile <- TRUE

for(system in systemDirs) {
  systemDir <- paste(workDir, "/", system, sep="")
  inFile <- paste(workDir, "/", system, "/", "summary.xlsx", sep="")
  if( firstFile ) {
    outputDF <- import(inFile)
  }
  else {
    tempDF <- import(inFile)
    outputDF[nrow(outputDF) + 1,] = tempDF[1,]
  }
  firstFile <- FALSE
}

print(outputDF)
export(outputDF, outFile)










