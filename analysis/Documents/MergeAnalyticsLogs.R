library("rio")

destDir <- "./Documents/RESULTS/load"
outFileName <- "mergedLoad.xlsx"
outFile <- paste(destDir, "/", outFileName, sep="")
workDir <- "./Documents/RESULTS/load"
searchedFile <- "analytics.xlsx"

#destDir <- "./Documents/RESULTS/power"
#outFileName <- "mergedPower.xlsx"
#outFile <- paste(destDir, "/", outFileName, sep="")
#workDir <- "./Documents/RESULTS/power"
#searchedFile <- "analytics.xlsx"

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

mergeXLSXFiles(workDir, searchedFile, outFile)












