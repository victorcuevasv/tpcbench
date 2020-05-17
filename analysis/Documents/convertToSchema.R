#args[1] bucket to mount
#args[2] s3 prefix for experiment files
#args[3] csv file with experiment labels (base name, should be located in Documents)

library("rio")
library("dplyr")

processAnalyticsFile <- function(inFile, experiment, test, instance, outFile) {
  analytics <- import(inFile, format="psv", colClasses="character")
  allColumns <- colnames(analytics)
  for( column in allColumns  ) {
    #Remove TUPLES and ITEM columns if they exist.
    if( column == "TUPLES" )
  	  analytics <- select(analytics, -TUPLES)
    if( column == "ITEM" )
  	  analytics <- select(analytics, -ITEM)
  }
  #Add the STREAM column with NA values if it does not exist.
  if( ! ("STREAM" %in% colnames(analytics)) ) {
    analytics["STREAM"] <- NA
  }
  #Reorder the columns
  analytics <- select(analytics, STREAM, QUERY, SUCCESSFUL,DURATION, RESULTS_SIZE, SYSTEM,
                      STARTDATE_EPOCH, STOPDATE_EPOCH, DURATION_MS, STARTDATE, STOPDATE)
  export(analytics, outFile, format="psv", na = "NaN", quote=FALSE)
}

processExperiments <- function(dirName, experiments, tests, instances) {
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        filePath <- paste0(dirName, experiment, "/", test, "/", instance, "/")
        dataFile <- paste0(filePath, "analytics.log")
        outFile <- paste0(filePath, "analyticsP.log")
        if( file.exists(dataFile) ) {
          processAnalyticsFile(dataFile, experiment, test, instance, outFile)
        }
      }
    }
  }
}

list.dirs <- function(path=".", pattern=NULL, all.dirs=FALSE,
                      full.names=FALSE, ignore.case=FALSE) {
  # use full.names=TRUE to pass to file.info
  all <- list.files(path, pattern, all.dirs,
                    full.names=TRUE, recursive=FALSE, ignore.case)
  dirs <- all[file.info(all)$isdir]
  # determine whether to return full names or just dir names
  if(isTRUE(full.names))
    return(dirs)
  else
    return(basename(dirs))
}

args <- commandArgs(TRUE)

dirName <- paste0("", args[1], "/", args[2])

#Option 1: use all of the subdirectories found in the directory.
experiments <- list.dirs(dirName)

#Option 2: use only the experiments listed in the provided file.
#experiments <- as.list(experimentsDF$EXPERIMENT)

tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('1', '2', '3')
                       
processExperiments(dirName, experiments, tests, instances)


