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

createPlotFromDataframe <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title, barsColor){
  dataf$TESTf <- factor(dataf$TEST, levels=c('load', 'loaddenorm', 'denormdeepcopy', 'denormmerge', 'billionints', 'writeunpartitioned', 'writepartitioned'))
  #dataf$TESTf <- factor(dataf$TEST, levels=c('load', 'power'))
  plot <- ggplot(data=dataf, aes(x=LABEL, y=get(metric), fill=TESTf), width=7, height=7) + 
    geom_bar(stat="identity", position = position_stack(reverse = T)) + 
    #It may be necessary to use 'fun.y = sum' instead of 'fun = sum' in some environments.
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = LABEL), stat = 'summary', fun.y = sum, vjust = -0.1, size=6) +     
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    scale_x_discrete(labels = function(x) str_wrap(x, width = 12)) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", metricsUnit[[metric]]), limits=c(0,metricsYaxisLimit[[metric]])) + 
    #scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")")) + 
    #The line below to species the colors manually -must provide enough colors-
    #scale_fill_manual(name="", values=c("#53986a", "#fdb863", "#53986a", "#e66101", "#53986a", "#a44747", "#53986a"), labels=c('read', 'upsert', 'read', 'delete', 'read', 'gdpr', 'read')) + 
    #scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#53986a", "#fdb863", "#53986a", "#e66101", "#53986a", "#a44747", "#53986a"), labels=c('load delta/hudi', 'z-order', 'read', 'upsert', 'read', 'delete', 'read', 'gdpr', 'read')) + 
    scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#ff5e78", "#fdb863", "#c5a880", "#e66101", "#53986a", "#a44747", "#53986a"), labels=c('load', 'loaddenorm', 'denormdeepcopy', 'denormmerge', 'billionints', 'writeunpartitioned', 'writepartitioned')) + 
    #scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#53986a", "#fdb863", "#53986a", "#e66101", "#53986a", "#a44747", "#53986a"), labels=c('load delta/hudi', 'upsert')) + 
    #scale_fill_manual(name = "", values=c(barsColor), labels=c('load', 'load_denorm', 'load_delta-hudi', 'zorder', 'insertdata', 'insupdtest', 'deletedata', 'deletetest', 'gdprtest')) +
    #scale_fill_manual(name="", values=c("greenyellow", "limegreen", "chartreuse4", "darkgreen"), labels=c('read1', 'read2', 'read3', 'read4')) +
    theme(legend.position = "bottom") + 
    #theme(legend.position = "none") + 
    theme(legend.text=element_text(size=14)) + 
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
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
        s3Suffix <- file.path(experiment, test, instance, "analytics.log")
        s3objURL <- URLencode(file.path(dirName, s3Suffix))
        if( file.exists(dataFile) ) {
          dataframe <- processAnalyticsFile(dataFile, dataframe, labels[[i]], experiment, test, instance, s3objURL)
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
metricsLabel[["AVG_TOTAL_DURATION_HOUR"]] <- "Total"
metricsLabel[["AVG_SPEEDUP"]] <- "Speedup vs \nEMR MoR"
metricsUnit<-new.env()
metricsUnit[["AVG_TOTAL_DURATION_HOUR"]] <- "(hr.)"
metricsUnit[["AVG_SPEEDUP"]] <- ""
metricsDigits<-new.env()
metricsDigits[["AVG_TOTAL_DURATION_HOUR"]] <- 2
metricsDigits[["AVG_SPEEDUP"]] <- 2
metricsYaxisLimit<-new.env()
metricsYaxisLimit[["AVG_TOTAL_DURATION_HOUR"]] <- 3.0
metricsYaxisLimit[["AVG_SPEEDUP"]] <- 2.0

args <- commandArgs(TRUE)
dirName <- file.path(args[1])
experiments <- NULL
labels <- NULL
tests <- list('load', 'loaddenorm', 'denormdeepcopy', 'denormmerge', 'billionints', 'writeunpartitioned', 'writepartitioned')
#tests <- list('load', 'power')
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
  
  dirNameElements <- c("s3://tpcds-results-test/dbr76/analytics")
  
  for(dirNameElement in dirNameElements) {
    df <- processExperimentsS3(dirNameElement, df, experiments, labels, tests, instances)
  }
}

outXlsxFile <- file.path(prefixOS, "Documents/experimentsAll.xlsx")
export(df, outXlsxFile)

df <- df %>%
  group_by(EXPERIMENT, LABEL, TEST, INSTANCE) %>%
  summarize(TOTAL_DURATION_HOUR = sum(DURATION, na.rm = TRUE) / 3600.0,
            TOTAL_DURATION_SEC = sum(DURATION, na.rm = TRUE),
            AVERAGE_DURATION_SEC = mean(DURATION, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = geoMean(DURATION, na.rm = TRUE))

dfSummary <- df %>%
  group_by(EXPERIMENT, LABEL, TEST) %>%
  summarize(AVG_TOTAL_DURATION_HOUR = mean(TOTAL_DURATION_HOUR, na.rm = TRUE),
            AVG_TOTAL_DURATION_SEC = mean(TOTAL_DURATION_SEC, na.rm = TRUE),
            AVG_DURATION_SEC = mean(AVERAGE_DURATION_SEC, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = mean(GEOMEAN_DURATION_SEC, na.rm = TRUE))

outXlsxFile <- file.path(prefixOS, "Documents/experiments.xlsx")
export(df, outXlsxFile)

outCSVFile <- file.path(prefixOS, "Documents/experiments.csv")
export(df, outCSVFile, na="NA", quote=FALSE)

outXlsxFile <- file.path(prefixOS, "Documents/experimentsSummary.xlsx")
export(dfSummary, outXlsxFile)

metric <- "AVG_TOTAL_DURATION_HOUR"
plot <- createPlotFromDataframe(dfSummary, metric, metricsLabel, metricsUnit, metricsDigits, 
                                "TPC-DS 3 TB ETL Benchmark", "skyblue4")
#outPngFile <- file.path(prefixOS, paste0("Documents/stacked_bar_chart_", tests[1], ".png"))
outPngFile <- file.path(prefixOS, paste0("Documents/stacked_bar_chart.png"))
#png(outPngFile, width=1200, height=300, res=120)
png(outPngFile, width=1200, height=600, res=120)
print(plot)
dev.off()

# emr530durationDF <- dfSummary[dfSummary$LABEL=="EMR Hudi MoR", "AVG_TOTAL_DURATION_HOUR"]
# emr530duration <- emr530durationDF$AVG_TOTAL_DURATION_HOUR[[1]]
# dfSummary$AVG_SPEEDUP <- emr530duration / dfSummary$AVG_TOTAL_DURATION_HOUR
# 
# dfSummaryFiltered <- dfSummary %>% filter(LABEL != 'EMR Hudi MoR')
# 
# metric <- "AVG_SPEEDUP"
# plot <- createPlotFromDataframe(dfSummaryFiltered, metric, metricsLabel, metricsUnit, metricsDigits,
#                                 "TPC-DS 1 TB DBR Delta vs. EMR Spark Hudi", "sandybrown")
# outPngFile <- file.path(prefixOS, paste0("Documents/stacked_bar_chart_", tests[1], "_speedup.png"))
# png(outPngFile, width=1200, height=300, res=120)
# print(plot)
# dev.off()

#For each test get the total time corresponding to each system to create a table.
#Get only the values and add the header later when converting to a dataframe.
table <- c()
toMinutes <- TRUE
fromSeconds <- TRUE
for(test in tests) {
  row <- c(test)
  for(label in labels) {
    t <- 0
    if( fromSeconds )
      t <- filter(dfSummary, LABEL == label & TEST == test)$AVG_TOTAL_DURATION_SEC
    else
      t <- filter(dfSummary, LABEL == label & TEST == test)$AVG_TOTAL_DURATION_HOUR
    if( length(t) == 0 )
      t <- 0
    if( toMinutes && ! fromSeconds )
      t <- t * 60
    else if( toMinutes && fromSeconds )
      t <- t / 60
    row <- c(row, t)
  }
  table <- c(table, list(row))
}

mat <- matrix(unlist(table), nrow=length(unlist(table[1])))
mat <- t(mat)
colnames(mat) <- c("test", labels)
tableDF <- as.data.frame(mat)
for(label in labels) {
  tableDF[[label]] <- as.numeric(as.character(tableDF[[label]]))
}

outXlsxFile <- file.path(prefixOS, "Documents/table.xlsx")
export(tableDF, outXlsxFile)



