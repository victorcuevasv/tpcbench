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

createBarChartFromDF <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title){
  #Use the factor function to transform the ScaleFactor column from continuous to categorical
  ggplot(data=dataf, aes(x=factor(QUERY), y=get(metric), fill=factor(LABEL))) +  
    #ggplot(data=dataf, aes(x=factor(QUERY), y=DURATION, label=round(DURATION, digits=metricsDigits[[metric]]))) +
    #The dodge position makes the bars appear side by side
    ggtitle(title) +
    theme(plot.title = element_text(size=18, face="bold")) +
    geom_bar(stat="identity", position=position_dodge()) +  
    #This line adds the exact values on top of the bars
    #geom_text(size = 5, position=position_dodge(width=1), vjust=0.1) +
    #facet_wrap(~ SYSTEM, ncol=2) +
    #geom_hline(aes(yintercept=SYSTEM_AVERAGE), data=dataf) +
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), 
                       limits=c(0,metricsYaxisLimit[[metric]])) + 
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    #scale_x_discrete(labels = function(x) str_wrap(x, width = 10)) + 
    #scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("EMR Presto", "Databricks Spark")) + 
    #This line adds the exact values on top of the bars
    #geom_text(aes(label=TPTSQL), position=position_dodge(width=0.9), vjust=-0.25)
    theme(legend.title = element_blank()) +
    #theme(legend.position = c(0.175, 0.85)) +
    theme(strip.text.x=element_text(size=18)) +
    theme(legend.text=element_text(size=14)) +
    theme(legend.position="bottom")
}

createBarChartMaxMinFromDF <- function(dataf, metric, metricMax, metricMin, metricsLabel, metricsUnit, 
                                       metricsDigits, title){
  ggplot(data=dataf, aes(x=factor(QUERY), y=get(metric), ymin=get(metricMin), ymax=get(metricMax), fill=factor(LABEL))) +
    ggtitle(title) +
    theme(plot.title = element_text(size=18, face="bold")) +
    geom_bar(stat="identity", position=position_dodge()) + 
    geom_errorbar(stat="identity", position=position_dodge(width = 0.9), width=0.3) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), 
                       limits=c(0,metricsYaxisLimit[[metric]])) + 
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
    theme(legend.title = element_blank()) +
    theme(strip.text.x=element_text(size=18)) +
    theme(legend.text=element_text(size=14)) +
    theme(legend.position="bottom")
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

getNdigit <- function(n, i) {
  t <- floor( (n %% 10^i) / (10^(i-1)) )
  return (t)
}

getRoundedLimit <- function(maxVal) {
  if( maxVal < 1.0 )
    return(1.0)
  limit <- 0
  nDigits <- ceiling(log10(maxVal))
  leftMostDigit <- getNdigit(maxVal, nDigits)
  if( leftMostDigit <= 8 )
    limit <- (leftMostDigit + 1) * 10^(nDigits-1)
  else
    limit <- 10^nDigits
  return (limit)
}

# Map metrics to descriptions for the y-axis.
metricsLabel<-new.env()
metricsLabel[["AVG_DURATION_HOUR"]] <- "Exec. time"
metricsLabel[["AVG_DURATION_SEC"]] <- "Exec. time"
metricsLabel[["AVG_DURATION_MIN"]] <- "Exec. time"

metricsUnit<-new.env()
metricsUnit[["AVG_DURATION_HOUR"]] <- "hr."
metricsUnit[["AVG_DURATION_SEC"]] <- "sec."
metricsUnit[["AVG_DURATION_MIN"]] <- "min."

metricsDigits<-new.env()
metricsDigits[["AVG_DURATION_HOUR"]] <- 2
metricsDigits[["AVG_DURATION_SEC"]] <- 0
metricsDigits[["AVG_DURATION_MIN"]] <- 1

metricsYaxisLimit<-new.env()
metricsYaxisLimit[["AVG_DURATION_HOUR"]] <- 1.0
metricsYaxisLimit[["AVG_DURATION_SEC"]] <- 3600
metricsYaxisLimit[["AVG_DURATION_MIN"]] <- 60



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
df <- data.frame(
                 EXPERIMENT=character(),
                 LABEL=character(),
                 TEST=character(),
                 INSTANCE=numeric(),
                 QUERY=numeric(),
                 STREAM=numeric(),
                 ITEM=numeric(),
                 STARTDATE_EPOCH=numeric(),
                 STOPDATE_EPOCH=numeric(),
                 DURATION_MS=numeric(),
                 DURATION=numeric(),
                 RESULTS_SIZE=numeric(),
                 TUPLES=numeric(),
                 stringsAsFactors=FALSE)

#Option 1: read the data from files in the local filesystem
if( ! startsWith(dirName, "s3:") ) {
  df <- processExperiments(dirName, df, experiments, labels, tests, instances)
} else {
  #Option 2: download the files from s3 and process them.
  #To process files in multiple paths, due probably to lazy evaluation it is absolutely necessary to
  #specify them beforehand, hardcoded.
  dirNameElements <- c(dirName)
  #dirNameElements <- c("s3://tpcds-results-test/emr529/analytics", "s3://tpcds-results-test/dbr65/analytics")
  for(dirNameElement in dirNameElements) {
    df <- processExperimentsS3(dirNameElement, df, experiments, labels, tests, instances)
  }
}

#Compute the ITEM, in case it was not specified in the data.
#df <- df %>% group_by(EXPERIMENT, LABEL, TEST, INSTANCE, STREAM) %>%
#  mutate(ITEM=with_order(order_by=STARTDATE_EPOCH, fun=row_number, x=STARTDATE_EPOCH))

#Compute the QUERY_RANK, i.e., the rank of the query in the test according to execution time.
#df <- df %>% group_by(EXPERIMENT, LABEL, TEST, QUERY) %>%
#  mutate(QUERY_RANK=with_order(order_by=DURATION, fun=row_number, x=DURATION))

outXlsxFile <- file.path(prefixOS, "Documents/rawdata.xlsx")
export(df, outXlsxFile)

df <- df %>%
  group_by(EXPERIMENT, LABEL, TEST, QUERY) %>%
  summarize(AVG_DURATION_SEC = mean(DURATION, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = geoMean(DURATION, na.rm = TRUE),
            MAX_DURATION_SEC = max(DURATION, na.rm = TRUE),
            MIN_DURATION_SEC = min(DURATION, na.rm = TRUE))
df$AVG_DURATION_HOUR <- df$AVG_DURATION_SEC / 3600.0
df$MAX_DURATION_HOUR <- df$MAX_DURATION_SEC / 3600.0
df$MIN_DURATION_HOUR <- df$MIN_DURATION_SEC / 3600.0
df$AVG_DURATION_MIN <- df$AVG_DURATION_SEC / 60.0
df$MAX_DURATION_MIN <- df$MAX_DURATION_SEC / 60.0
df$MIN_DURATION_MIN <- df$MIN_DURATION_SEC / 60.0

outXlsxFile <- file.path(prefixOS, "Documents/experiments.xlsx")
export(df, outXlsxFile)

metric <- "AVG_DURATION_MIN"
metricMax <- "MAX_DURATION_MIN"
metricMin <- "MIN_DURATION_MIN"
yAxisDefaultLimit <- metricsYaxisLimit[[metric]]
for( test in tests  ) {
  title <- paste0("TPC-DS ", test, " test at 1 TB")
  dfFiltered <- df[df$TEST == test,]
  queriesPerChart <- 25
  iterations <- ceiling(n_distinct(dfFiltered$QUERY) / queriesPerChart)
  plotMaxMin <- TRUE
  for(i in 1:iterations) {
    lower <- (i-1) * queriesPerChart + 1
    upper <- i * queriesPerChart
    dfSubset <- dfFiltered[dfFiltered$QUERY >= lower & dfFiltered$QUERY <= upper,]
    if( ! plotMaxMin ) {
      metricsYaxisLimit[[metric]] <- getRoundedLimit(max(dfFiltered[[metric]]))
      plot <- createBarChartFromDF(dfSubset, metric, metricsLabel, metricsUnit, metricsDigits, title)
    } else {
      metricsYaxisLimit[[metric]] <- getRoundedLimit(max(dfFiltered[[metricMax]]))
      plot <- createBarChartMaxMinFromDF(dfSubset, metric, metricMax, metricMin, metricsLabel, metricsUnit, metricsDigits, title)
    }
    outPngFile <- file.path(prefixOS, paste0("Documents/bar_chart_", test, "_", i, ".png"))
    png(outPngFile, width=1400, height=800, res=120)
    print(plot)
    dev.off()
  }
}
metricsYaxisLimit[[metric]] <- yAxisDefaultLimit


