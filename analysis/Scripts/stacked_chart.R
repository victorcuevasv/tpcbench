#!/usr/bin/env Rscript
#args[1] results data directory (can be mounted s3 bucket)
#args[2] csv file with experiment labels (should be located in Documents - optional)

#Use for running locally
prefixOS <- getwd()
#Change if running in Docker
if( prefixOS == "/" )
  prefixOS <- "/home/rstudio"

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-west-2")

options(repr.plot.width=1500, repr.plot.height=400)

#Next line with INSTALL_opts may be necessary on windows.
#install.packages("aws.s3", repos = c("cloudyr" = "https://cloud.R-project.org"), INSTALL_opts = "--no-multiarch")
library("aws.s3")
library("rio")
library("ggplot2")
library("stringr")
library("dplyr")
library("EnvStats")

processAnalyticsFile <- function(inFile, dataframe, label, experiment, test, instance) {
  analytics <- import(inFile, format="psv", colClasses="character")
  #Convert the character colums to numeric values where needed.
  analytics$QUERY <- as.numeric(analytics$QUERY)
  #analytics$STREAM <- as.numeric(analytics$STREAM)
  analytics$STARTDATE_EPOCH <- as.numeric(analytics$STARTDATE_EPOCH)
  analytics$STOPDATE_EPOCH <- as.numeric(analytics$STOPDATE_EPOCH)
  analytics$DURATION_MS <- as.numeric(analytics$DURATION_MS)
  analytics$DURATION <- as.numeric(analytics$DURATION)
  analytics$RESULTS_SIZE <- as.numeric(analytics$RESULTS_SIZE)
  #analytics$TUPLES <- as.numeric(analytics$TUPLES)
  return(calculateStats(analytics, dataframe, label, experiment, test, instance))
}

calculateStats <- function(analytics, dataframe, label, experiment, test, instance) {
  totalDurationSec <- 0
  if( test == "tput" )
    totalDurationSec <- (max(analytics$STOPDATE_EPOCH) - min(analytics$STARTDATE_EPOCH)) / 1000.0
  else
    totalDurationSec <- sum(analytics$DURATION)
  totalDurationHour <- totalDurationSec / 3600.0
  averageDurationSec <- mean(analytics$DURATION)
  geomeanDurationSec <- geoMean(analytics$DURATION)
  dataframe[nrow(dataframe) + 1,] = list(label, test, instance, totalDurationSec, totalDurationHour, 
                                         averageDurationSec, geomeanDurationSec)
  return(dataframe)
}

createPlotFromDataframe <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title){
  plot <- ggplot(data=dataf, aes(x=EXPERIMENT, y=get(metric), fill=TEST), width=7, height=7) + 
    geom_bar(stat="identity", position = position_stack(reverse = T)) + 
    #It may be necessary to use 'fun.y = sum' instead of 'fun = sum' in some environments.
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = EXPERIMENT), stat = 'summary', fun.y = sum, vjust = -0.1, size=6) +     
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    scale_x_discrete(labels = function(x) str_wrap(x, width = 20)) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), limits=c(0,metricsYaxisLimit[[metric]])) + 
    #scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")")) + 
    #The line below to species the colors manually -must provide enough colors-
    scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#fdb863", "#e66101"), labels=c('load', 'analyze', 'power', 'tput')) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) + 
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
  return(plot)
}

processExperimentsS3 <- function(dirName, dataframe, experiments, labels, tests, instances) {
  i <- 1
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        s3Suffix <- file.path(experiment, test, instance, "analytics.log")
        #It is NOT necessary to add s3://
        s3objURL <- URLencode(file.path(dirName, s3Suffix))
        if( object_exists(s3objURL) ) {
          dataFile <- "data.txt"
          saveS3ObjectToFile(s3objURL, dataFile)
          dataframe <- processAnalyticsFile(dataFile, dataframe, labels[[i]], experiment, test, instance)
        }
      }
    }
    i <- i + 1
  }
  return(dataframe)
}

saveS3ObjectToFile <- function(s3objURL, outFile) {
  s3obj <- get_object(s3objURL)
  charObj <- rawToChar(s3obj)
  sink(outFile)
  cat (charObj)
  sink()
}

processExperiments <- function(dirName, dataframe, experiments, labels, tests, instances) {
  i <- 1
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        fileSuffix <- file.path(experiment, test, instance, "analytics.log")
        dataFile <- file.path(prefixOS, dirName, fileSuffix)
        if( file.exists(dataFile) ) {
          dataframe <- processAnalyticsFile(dataFile, dataframe, labels[[i]], experiment, test, instance)
        }
      }
    }
    i <- i + 1
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

readExperimentsAsDataframe <- function(inFile) {
  print(inFile)
  experimentsDF <- import(inFile, format="csv", colClasses="character")
  return(experimentsDF)
}

createLabelsList <- function(experimentsDF, experimentsList) {
  labels <- list()
  i <- 1
  for(experiment in experimentsList) {
    labels[[i]] <- experimentsDF$LABEL[experimentsDF$EXPERIMENT == experiment]
    i <- i + 1
  }
  return(labels)
}

# Map metrics to descriptions for the y-axis.
metricsLabel<-new.env()
metricsLabel[["AVG_TOTAL_DURATION_HOUR"]] <- "Total"
metricsUnit<-new.env()
metricsUnit[["AVG_TOTAL_DURATION_HOUR"]] <- "hr."
metricsDigits<-new.env()
metricsDigits[["AVG_TOTAL_DURATION_HOUR"]] <- 2
metricsYaxisLimit<-new.env()
metricsYaxisLimit[["AVG_TOTAL_DURATION_HOUR"]] <- 30.0

args <- commandArgs(TRUE)
dirName <- file.path(args[1])
experiments <- NULL
labels <- NULL
tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('1', '2', '3')

#Option 1: use all of the subdirectories found in the directory.
#For the labels, use the subdirectory name.
if( length(args) < 2 ) {
  experiments <- list.dirs(dirName)
  labels <- experiments
} else {
  #Option 2: use only the experiments listed in the provided file.
  #Those that are commented will be ignored.
  #
  #Example file listing the experiments:
  #
  #EXPERIMENT,LABEL
  #s3://1bcjobaw6vl7h6y6zxlmm7zxom7n9gx/analytics,NA (line commented in the actual file)
  #snowflakelarge128stream1clusterdefconcimpalakit,128streams
  #snowflakelarge16stream1clusterdefconcimpalakit,16streams
  #...
  experimentsDF <- readExperimentsAsDataframe(file.path(prefixOS, "Documents", args[2]))
  experiments <- as.list(experimentsDF$EXPERIMENT)
  labels <- createLabelsList(experimentsDF, experiments)
}

#Create a new dataframe to hold the aggregate results, pass it to the various functions.
df <- data.frame(EXPERIMENT=character(),
                 TEST=character(),
                 INSTANCE=character(),
                 TOTAL_DURATION_SEC=double(),
                 TOTAL_DURATION_HOUR=double(),
                 AVERAGE_DURATION_SEC=double(),
                 GEOMEAN_DURATION_SEC=double(),
                 stringsAsFactors=FALSE)

#Option 1: read the data from files in the local filesystem
if( ! startsWith(dirName, "s3:") ) {
  df <- processExperiments(dirName, df, experiments, labels, tests, instances)
} else {
  #Option 2: download the files from s3 and process them.
  #To process files in multiple paths, due probably to lazy evaluation it is absolutely necessary to
  #specify them beforehand, hardcoded.
  #dirNameElements <- c(dirName)
  #dirNameElements <- c("s3://tpcds-results-test/emr529/analytics", "s3://tpcds-results-test/dbr65/analytics")
  dirNameElements <- c("s3://tpcds-results-obsolete/emrmysqldriver/analytics")
  for(dirNameElement in dirNameElements) {
    df <- processExperimentsS3(dirNameElement, df, experiments, labels, tests, instances)
  }
}

df <- df %>%
  group_by(EXPERIMENT, TEST) %>%
  summarize(AVG_TOTAL_DURATION_HOUR = mean(TOTAL_DURATION_HOUR, na.rm = TRUE),
            AVG_DURATION_SEC = mean(AVERAGE_DURATION_SEC, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = mean(GEOMEAN_DURATION_SEC, na.rm = TRUE))

outXlsxFile <- file.path(prefixOS, "Documents/experiments.xlsx")
export(df, outXlsxFile)

metric <- "AVG_TOTAL_DURATION_HOUR"
plot <- createPlotFromDataframe(df, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB")
outPngFile <- file.path(prefixOS, "Documents/stacked_bar_chart.png")
png(outPngFile, width=1500, height=500, res=120)
print(plot)
dev.off()

