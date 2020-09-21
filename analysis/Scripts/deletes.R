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

createPlotFromDataframePowerTest <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title){
  #Use the factor function to transform the ScaleFactor column from continuous to categorical
  plot <- ggplot(data=dataf, aes(x=factor(QUERY), y=get(metric), fill=factor(LABEL))) +  
    #ggplot(data=dataf, aes(x=factor(QUERY), y=DURATION, label=round(DURATION, digits=metricsDigits[[metric]]))) +
    #The dodge position makes the bars appear side by side
    geom_bar(stat="identity", position=position_dodge()) +  
    #This line adds the exact values on top of the bars
    #geom_text(size = 5, position=position_dodge(width=1), vjust=0.1) +
    #facet_wrap(~ SYSTEM, ncol=2) +
    #geom_hline(aes(yintercept=SYSTEM_AVERAGE), data=dataf) +
    ylab("Exec. time (sec.)") +
    ylim(0, metricsYaxisLimit[[metric]]) +
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    #scale_x_discrete(labels = function(x) str_wrap(x, width = 10)) + 
    #scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#fdb863", "#e66101", "#a44747")) +  
    #This line adds the exact values on top of the bars
    #geom_text(aes(label=TPTSQL), position=position_dodge(width=0.9), vjust=-0.25)
    ggtitle(title) +
    theme(plot.title = element_text(size=20, face="bold")) +
    theme(legend.title = element_blank()) +
    #theme(legend.position = c(0.175, 0.85)) +
    theme(strip.text.x=element_text(size=18)) +
    theme(legend.text=element_text(size=14)) +
    theme(legend.position="bottom")
  return(plot)
}

processAnalyticsFile <- function(inFile, dataframe, label, experiment, test, instance, s3objURL) {
  analytics <- import(inFile, format="psv", colClasses="character")
  analyticsCols <- colnames(analytics)
  dataframeCols <- colnames(dataframe)
  dataframeColsTypes <- sapply(dataframe, class)
  for(i in 1:length(dataframeCols)) {
    dataframeCol <- dataframeCols[[i]]
    if( dataframeCol %in% analyticsCols ) {
      #Cannot use $ notation when using a variable, must use [[]]
      if( dataframeColsTypes[[i]] == "numeric" )
        analytics[[dataframeCol]] <- as.numeric(analytics[[dataframeCol]])
    }
    else if( ! (dataframeCol %in% analyticsCols) ) {
      if( dataframeCol == "EXPERIMENT" )
        analytics[dataframeCol] <- experiment
      else if ( dataframeCol == "LABEL" )
        analytics[dataframeCol] <- label
      else if ( dataframeCol == "TEST" )
        analytics[dataframeCol] <- test
      else if ( dataframeCol == "INSTANCE" )
        analytics[dataframeCol] <- as.numeric(instance)
      else if ( dataframeCol == "URL" )
        analytics[dataframeCol] <- s3objURL
      else {
        if( dataframeColsTypes[[i]] == "numeric" )
          analytics[dataframeCol] <- NaN
        else
          analytics[dataframeCol] <- "NA"
      }
    }
  }
  for(analyticsCol in analyticsCols) {
    if( ! (analyticsCol %in% dataframeCols) ) {
      analytics <- select(analytics, -analyticsCol)
    }
  }
  return(union(dataframe, analytics))
}


processExperimentsS3 <- function(dirName, dataframe, experiments, labels, tests, instances) {
  i <- 1
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        s3Suffix <- file.path(experiment, test, instance, "analytics.log")
        #It is NOT necessary to add s3://
        s3objURL <- URLencode(file.path(dirName, s3Suffix))
        print(s3objURL)
        if( object_exists(s3objURL) ) {
          #print(s3objURL)
          #if( experiment == "prestoemr-529-8nodes-1000gb-hiveparquet1589503121" && instance > 1 )
          #  next
          dataFile <- "data.txt"
          saveS3ObjectToFile(s3objURL, dataFile)
          dataframe <- processAnalyticsFile(dataFile, dataframe, labels[[i]], experiment, test, instance, s3objURL)
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
  #experimentsDF <- import(inFile, format="csv", colClasses="character")
  experimentsDF <- readr::read_delim(inFile, comment = '#', delim = ',')
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
metricsLabel[["AVG_DURATION_SEC"]] <- "Total"
metricsLabel[["AVG_TOTAL_DURATION_HOUR"]] <- "Total"
metricsLabel[["AVG_SPEEDUP"]] <- "Speedup vs \nEMR MoR"
metricsUnit<-new.env()
metricsUnit[["AVG_DURATION_SEC"]] <- "sec."
metricsUnit[["AVG_TOTAL_DURATION_HOUR"]] <- "(hr.)"
metricsUnit[["AVG_SPEEDUP"]] <- ""
metricsDigits<-new.env()
metricsDigits[["AVG_DURATION_SEC"]] <- 2
metricsDigits[["AVG_TOTAL_DURATION_HOUR"]] <- 2
metricsDigits[["AVG_SPEEDUP"]] <- 2
metricsYaxisLimit<-new.env()
metricsYaxisLimit[["AVG_DURATION_SEC"]] <- 1500
metricsYaxisLimit[["AVG_TOTAL_DURATION_HOUR"]] <- 10.0
metricsYaxisLimit[["AVG_SPEEDUP"]] <- 1.5

args <- commandArgs(TRUE)
dirName <- file.path(args[1])
experiments <- NULL
labels <- NULL
#tests <- list('load', 'loaddenorm', 'loadupdate', 'zorderupdate', 'insertdata', 'insupdtest', 'deletedata', 'deletetest', 'gdprtest')
#tests <- list('loadupdate', 'zorderupdate', 'insupdtest', 'deletetest', 'gdprtest')
tests <- list('gdprtest')
instances <- list('1')

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
  print(experiments)
  labels <- createLabelsList(experimentsDF, experiments)
}

#Create a new dataframe to hold the aggregate results, pass it to the various functions.
df <- data.frame(
                 EXPERIMENT=character(),
                 SYSTEM=character(),
                 TEST=character(),
                 INSTANCE=numeric(),
                 QUERY=numeric(),
                 STREAM=numeric(),
                 ITEM=numeric(),
                 DURATION_MS=numeric(),
                 DURATION=numeric(),
                 STARTDATE_EPOCH=numeric(),
                 STOPDATE_EPOCH=numeric(),
                 STARTDATE=character(),
                 STOPDATE=character(),
                 SUCCESSFUL=character(),
                 RESULTS_SIZE=numeric(),
                 TUPLES=numeric(),
                 SCALE_FACTOR=numeric(),
                 ITERATION=numeric(),
                 LABEL=character(),
                 URL=character(),
                 stringsAsFactors=FALSE)

#Option 1: read the data from files in the local filesystem
if( ! startsWith(dirName, "s3:") ) {
  df <- processExperiments(dirName, df, experiments, labels, tests, instances)
} else {
  #Option 2: download the files from s3 and process them.
  #To process files in multiple paths, due probably to lazy evaluation it is absolutely necessary to
  #specify them beforehand, hardcoded.
  #dirNameElements <- c(dirName)
  #dirNameElements <- c("s3://1-rtvjs-45qnx2peo-ar39q2dprvzkmga/analytics")
  
  dirNameElements <- c("s3://tpcds-results-test/dbr72/analytics",
                       "s3://tpcds-results-test/sparkemr-530-test/analytics")
  
  #dirNameElements <- c("s3://tpcds-results-test/dbr70/analytics")
  
  for(dirNameElement in dirNameElements) {
    df <- processExperimentsS3(dirNameElement, df, experiments, labels, tests, instances)
  }
}

outXlsxFile <- file.path(prefixOS, "Documents/experimentsAll.xlsx")
export(df, outXlsxFile)

df <- df %>%
  group_by(EXPERIMENT, LABEL, TEST, INSTANCE, QUERY) %>%
  summarize(AVG_DURATION_SEC = mean(DURATION, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = geoMean(DURATION, na.rm = TRUE))

outXlsxFile <- file.path(prefixOS, "Documents/experiments.xlsx")
export(df, outXlsxFile)

outCSVFile <- file.path(prefixOS, "Documents/experiments.csv")
export(df, outCSVFile, na="NA", quote=FALSE)

metric <- "AVG_DURATION_SEC"
#plot <- createPlotFromDataframePowerTest(df, metric, metricsLabel, metricsUnit, metricsDigits, 
#                                 "Delete Test (1: 0.1%, 2: 1%, 3: 11.1%)")
plot <- createPlotFromDataframePowerTest(df, metric, metricsLabel, metricsUnit, metricsDigits, 
                                         "GDPR Test on Hudi (1: query, 2: write)")
outPngFile <- file.path(prefixOS, paste0("Documents/bar_chart.png"))
png(outPngFile, width=1200, height=600, res=120)
print(plot)
dev.off()


