#args[1] bucket to mount
#args[2] s3 prefix for experiment files

library("rio")
library("dplyr")

processAnalyticsFile <- function(inFile, experiment, test, instance) {

	df <- data.frame(experiment=character(),
                       test=character(),
                       stream=character(),
                       instance=character(),
                       query=character(),
                       successful=character(),
                       duration=character(),
                       results_size=character(),
                       system=character(),
                       startdate_epoch=character(),
                       stopdate_epoch=character(),
                       duration_ms=character(),
                       startdate=character(),
                       stopdate=character(),
                       stringsAsFactors=FALSE)

  analytics <- import(inFile, format="psv", colClasses="character")
  #Remove TUPLES and ITEM columns if they exist.
  if( "TUPLES" %in% colnames(analytics) )
  	analytics <- select(analytics, -TUPLES)
  if( "ITEM" %in% colnames(analytics) )
  	analytics <- select(analytics, -ITEM)
  export(analytics, inFile, format="psv")
}

processExperiments <- function(dirName, experiments, tests, instances) {
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        fileSuffix <- paste0(experiment, "/", test, "/", instance, "/analytics.log")
        dataFile <- paste0(dirName, fileSuffix)
        if( file.exists(dataFile) ) {
          processAnalyticsFile(dataFile, experiment, test, instance)
        }
      }
    }
  }
  return(dataframe)
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

dirName <- "/home/rstudio/Documents/analytics/"

#Option 1: use all of the subdirectories found in the directory.
experiments <- list.dirs(dirName)

tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('1', '2', '3')
                       
processExperiments(dirName, experiments, tests, instances)


