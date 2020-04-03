library("rio")

#######################################################
testsToDo <- c("load", "power", "tput")
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
      for( row in 1:nrow(tempDF)  )
        outputDF[nrow(outputDF) + 1,] = tempDF[row,]
    }
    firstFile <- FALSE
  }
  print(outputDF)
  export(outputDF, outFile)
}

doTest <- function(testDone) {
  print(paste0("Processing files for the ", testDone, " test."))
  destDir <- paste0("./Documents/RESULTS/", testDone)
  outFileName <- paste0("merged_", testDone, ".xlsx")
  outFile <- paste(destDir, "/", outFileName, sep="")
  workDir <- paste0("./Documents/RESULTS/", testDone)
  searchedFile <- "analytics.xlsx"
  mergeXLSXFiles(workDir, searchedFile, outFile)
}












