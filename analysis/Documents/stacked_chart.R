#args[1] bucket to mount
#args[2] s3 prefix for experiment files
#args[3] csv file with experiment labels (base name, should be located in Documents)

options(repr.plot.width=1500, repr.plot.height=400)

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
  plot <- ggplot(data=dataf, aes(x=SYSTEM, y=get(metric), fill=TEST), width=7, height=7) + 
    geom_bar(stat="identity", position = position_stack(reverse = T)) + 
    #It may be necessary to use 'fun.y = sum' instead of 'fun = sum' in some environments.
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = SYSTEM), stat = 'summary', fun = sum, vjust = -0.1, size=6) +     
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    scale_x_discrete(labels = function(x) str_wrap(x, width = 30)) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), limits=c(0,metricsYaxisLimit[[metric]])) + 
    #scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")")) + 
    #Use the line below to specify the colors manually -must provide enough colors-
    scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#fdb863", "#e66101"), labels=c('load', 'analyze', 'power', 'tput')) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) + 
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
  return(plot)
}

processExperimentsS3 <- function(dataframe, experiments, labels, tests, instances) {
  i <- 1
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        s3Suffix <- paste0(experiment, "/", test, "/", instance, "/analytics.log")
        s3objURL <- paste0(s3Prefix, s3Suffix)
        if( object_exists(s3objURL) ) {
          dataFile <- "/home/rstudio/Documents/data.txt"
          saveS3ObjectToFile(s3objURL, dataFile)
          dataframe <- processAnalyticsFile(dataFile, dataframe, labels[[i]], experiment, test, instance)
        }
      }
    }
    i <- i + 1
  }
  return(dataframe)
}

processExperiments <- function(dirName, dataframe, experiments, labels, tests, instances) {
  i <- 1
  for(experiment in experiments) {
    for(test in tests) {
      for(instance in instances) {
        fileSuffix <- paste0(experiment, "/", test, "/", instance, "/analytics.log")
        dataFile <- paste0(dirName, fileSuffix)
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

metric <- "AVG_TOTAL_DURATION_HOUR"

#s3Prefix <- "s3://presto-comp/analytics/"

args <- commandArgs(TRUE)

dirName <- paste0("/home/rstudio/s3buckets/", args[1], "/", args[2])

experimentsDF <- readExperimentsAsDataframe(paste0("/home/rstudio/Documents/", args[3]))

#Option 1: use all of the subdirectories found in the directory.
#experiments <- list.dirs(dirName)

#Option 2: use only the experiments listed in the provided file.
experiments <- as.list(experimentsDF$EXPERIMENT)

#print(experiments)

labels <- createLabelsList(experimentsDF, experiments)

#print(labels)

#Create a new dataframe to hold the aggregate results.
df <- data.frame(SYSTEM=character(),
                       TEST=character(),
                       INSTANCE=character(),
                       TOTAL_DURATION_SEC=double(),
                       TOTAL_DURATION_HOUR=double(),
                       AVERAGE_DURATION_SEC=double(),
                       GEOMEAN_DURATION_SEC=double(),
                       stringsAsFactors=FALSE)

tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('1', '2', '3')

df <- processExperiments(dirName, df, experiments, labels, tests, instances)

df <- df %>%
  group_by(SYSTEM, TEST) %>%
  summarize(AVG_TOTAL_DURATION_HOUR = mean(TOTAL_DURATION_HOUR, na.rm = TRUE),
            AVG_DURATION_SEC = mean(AVERAGE_DURATION_SEC, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = mean(GEOMEAN_DURATION_SEC, na.rm = TRUE))

#print(df, n=nrow(df))

export(df, "/home/rstudio/Documents/experiments.xlsx")

plot <- createPlotFromDataframe(df, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB")

png("/home/rstudio/Documents/stacked_bar_chart.png", width=1500, height=500, res=120)
print(plot)
dev.off()

