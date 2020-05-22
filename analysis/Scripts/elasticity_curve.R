#!/usr/bin/env Rscript
#This script creates a stacked chart, an elasiticy curve, and a speedup curve.
#args[1] results data directory (can be mounted bucket) - required
#args[2] csv file with experiment labels (should be located in Documents) - required

#Use for running locally
prefixOS <- getwd()
#Change if running in Docker
if( prefixOS == "/" )
  prefixOS <- "/home/rstudio"

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-west-2")

#options(repr.plot.width=1500, repr.plot.height=400)

#Next line with INSTALL_opts may be necessary on windows.
#install.packages("aws.s3", repos = c("cloudyr" = "https://cloud.R-project.org"), INSTALL_opts = "--no-multiarch")
library("aws.s3")
library("rio")
library("ggplot2")
library("stringr")
library("dplyr")
library("EnvStats")

processAnalyticsFile <- function(inFile, dataframe, experimentsDF, experiment, test, instance) {
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
  return(calculateStats(analytics, dataframe, experimentsDF, experiment, test, instance))
}

calculateStats <- function(analytics, dataframe, experimentsDF, experiment, test, instance) {
  totalDurationSec <- 0
  if( test == "tput" )
    totalDurationSec <- (max(analytics$STOPDATE_EPOCH) - min(analytics$STARTDATE_EPOCH)) / 1000.0
  else
    totalDurationSec <- sum(analytics$DURATION)
  totalDurationHour <- totalDurationSec / 3600.0
  averageDurationSec <- mean(analytics$DURATION)
  geomeanDurationSec <- geoMean(analytics$DURATION)
  label <- experimentsDF$LABEL[experimentsDF$EXPERIMENT == experiment] 
  size <- experimentsDF$SIZE[experimentsDF$EXPERIMENT == experiment]
  nodes <- experimentsDF$NODES[experimentsDF$EXPERIMENT == experiment]
  system <- experimentsDF$SYSTEM[experimentsDF$EXPERIMENT == experiment]
  dataframe[nrow(dataframe) + 1,] = list(label, size, nodes, system, experiment, test, instance, totalDurationSec, totalDurationHour, 
                                         averageDurationSec, geomeanDurationSec)
  return(dataframe)
}

createStackedChartFromDF <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title){
  plot <- ggplot(data=dataf, aes(x=LABEL, y=get(metric), fill=TEST), width=7, height=7) + 
    geom_bar(stat="identity", position = position_stack(reverse = T)) + 
    #It may be necessary to use 'fun.y = sum' instead of 'fun = sum' in some environments.
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = LABEL), stat = 'summary', fun.y = sum, vjust = -0.1, size=6) +     
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    scale_x_discrete(labels = function(x) str_wrap(x, width = 30)) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), limits=c(0,metricsYaxisLimit[[metric]])) + 
    #scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")")) + 
    #The line below specifies the colors manually -must provide enough colors-
    scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#fdb863", "#e66101"), labels=c('load', 'analyze', 'power', 'tput')) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) + 
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
  return(plot)
}

createElasticityPlotFromDF <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title, useDiscrete) {
  xObj <- NULL
  xScale <- NULL
  if( useDiscrete ) {
    xObj <- factor(dataf$SIZE, levels=c('xs-1n', 's-2n', 'M-4n', 'L-8n', 'XL-16n'))
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    xScale <- scale_x_discrete("Cluster size", labels = function(x) str_wrap(x, width = 11))
  } else {
    xObj <- dataf$NODES
    xScale <- scale_x_continuous("Nodes", limits=c(0,20), breaks=seq(0,20,2))
  }
  ggplot(data=dataf, aes(x=xObj, 
                         y=get(metric), color=SYSTEM, linetype=SYSTEM, group=SYSTEM), width=7, height=7) +  
    geom_line() +
    geom_point() +
    ggtitle(title) +
    theme(plot.title = element_text(size=20, face="bold")) +
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = SYSTEM),
              vjust = -1, size=5, show.legend = FALSE) +
    #Use to eliminate the x-axis title
    #theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=14), axis.title=element_text(size=16)) +
    xScale +
    #Use the line below to specify the limit for the y axis
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), limits=c(0,metricsYaxisLimit[[metric]])) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) +  
    theme(legend.title=element_blank()) +
    scale_color_discrete(name="", labels = c("DBR 6.1", "SFK")) +
    scale_linetype_discrete(name="", labels = c("DBR 6.1", "SFK")) +
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
}

createSpeedupPlotFromDF <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title, useDiscrete) {
  xObj <- NULL
  xScale <- NULL
  if( useDiscrete ) {
    xObj <- factor(dataf$SIZE, levels=c('xs-1n', 's-2n', 'M-4n', 'L-8n', 'XL-16n'))
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    xScale <- scale_x_discrete("Cluster size", labels = function(x) str_wrap(x, width = 11))
  } else {
    xObj <- dataf$NODES.x
    xScale <- scale_x_continuous("Nodes", limits=c(0,20), breaks=seq(0,20,2))
  }
  #Note that it is necessary to add group=1 in order to be able to draw the line.
  ggplot(data=dataf, aes(x=xObj,
                         y=get(metric), group=1, width=7, height=7)) +  
    geom_line() +
    geom_point() +
    ggtitle(title) +
    theme(plot.title = element_text(size=20, face="bold")) +
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]])),
              vjust = -1, size=5, show.legend = FALSE) +
    theme(axis.text=element_text(size=14), axis.title=element_text(size=16)) +
    xScale + 
    #Use the line below to specify the limit for the y axis
    scale_y_continuous(metricsLabel[[metric]], limits=c(0,metricsYaxisLimit[[metric]])) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) +  
    theme(legend.title=element_blank()) +
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
}

processExperimentsS3 <- function(dirName, dataframe, experimentsDF, tests, instances) {
  i <- 1
  for(experiment in as.list(experimentsDF$EXPERIMENT)) {
    for(test in tests) {
      for(instance in instances) {
        s3Suffix <- file.path(experiment, test, instance, "analytics.log")
        #It is NOT necessary to add s3://
        s3objURL <- file.path(dirName, s3Suffix)
        if( object_exists(s3objURL) ) {
          dataFile <- "data.txt"
          saveS3ObjectToFile(s3objURL, dataFile)
          dataframe <- processAnalyticsFile(dataFile, dataframe, experimentsDF, experiment, test, instance)
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

readExperimentsAsDataframe <- function(inFile) {
  print(inFile)
  experimentsDF <- import(inFile, format="csv", colClasses="character")
  return(experimentsDF)
}

# Map metrics to descriptions for the y-axis.
metricsLabel<-new.env()
metricsLabel[["TOTAL_DURATION_HOUR"]] <- "Total"
metricsLabel[["TOTAL_DURATION_SEC"]] <- "Total"
metricsLabel[["AVG_TOTAL_DURATION_HOUR"]] <- "Total"
metricsLabel[["SPEEDUP"]] <- ""

metricsUnit<-new.env()
metricsUnit[["TOTAL_DURATION_HOUR"]] <- "hr."
metricsUnit[["TOTAL_DURATION_SEC"]] <- "sec."
metricsUnit[["AVG_TOTAL_DURATION_HOUR"]] <- "hr."
metricsUnit[["SPEEDUP"]] <- ""

metricsDigits<-new.env()
metricsDigits[["TOTAL_DURATION_HOUR"]] <- 2
metricsDigits[["TOTAL_DURATION_SEC"]] <- 2
metricsDigits[["AVG_TOTAL_DURATION_HOUR"]] <- 2
metricsDigits[["SPEEDUP"]] <- 2

metricsYaxisLimit<-new.env()
metricsYaxisLimit[["TOTAL_DURATION_HOUR"]] <- 20.0
metricsYaxisLimit[["TOTAL_DURATION_SEC"]] <- 9000
metricsYaxisLimit[["AVG_TOTAL_DURATION_HOUR"]] <- 20.0
metricsYaxisLimit[["SPEEDUP"]] <- 3.0

#metric <- "TOTAL_DURATION_HOUR"
metric <- "AVG_TOTAL_DURATION_HOUR"
#metric <- "SPEEDUP"

args <- commandArgs(TRUE)
dirName <- file.path(args[1])
experimentsDF <- NULL
tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('0', '1', '2', '3')

#Example of an experiments file.

#EXPERIMENT,LABEL,SIZE,NODES,SYSTEM
#s3://1-rtvjs-45qnx2peo-ar39q2dprvzkmga/analytics,NA,NA,NA,NA (line commented in actual file)
#snowflakesmallonecluster,SFK-s,s-2n,2,SNOWFLAKE
#snowflakemediumonecluster,SFK-M,M-4n,4,SNOWFLAKE
#snowflakelargeonecluster,SFK-L,L-8n,8,SNOWFLAKE
#snowflakexlargeonecluster,SFK-XL,XL-16n,16,SNOWFLAKE
#databricks61delta2wpartnoz,DBR-2w,s-2n,2,DBR_DLT_NOZ_PART 
#databricks61delta4wpartnoz,DBR-4w,M-4n,4,DBR_DLT_NOZ_PART
#databricks61delta8wpartnoz,DBR-8w,L-8n,8,DBR_DLT_NOZ_PART
#databricks61delta16wpartnoz,DBR-16w,XL-16n,16,DBR_DLT_NOZ_PART

if( length(args) < 2 )
  stop("Usage: elasticity_curve.R <results dir> <csv file listing experiments>.")

experimentsDF <- readExperimentsAsDataframe(file.path(prefixOS, "Documents", args[2]))

#Create a new dataframe to hold the aggregate results, pass it to the various functions.
df <- data.frame(LABEL=character(),
                 SIZE=character(),
                 NODES=numeric(),
                 SYSTEM=character(),
                 EXPERIMENT=character(),
                 TEST=character(),
                 INSTANCE=character(),
                 TOTAL_DURATION_SEC=double(),
                 TOTAL_DURATION_HOUR=double(),
                 AVERAGE_DURATION_SEC=double(),
                 GEOMEAN_DURATION_SEC=double(),
                 stringsAsFactors=FALSE)

#Option 1: read the data from files in the local filesystem
if( ! startsWith(args[1], "s3:") ) {
  df <- processExperiments(dirName, df, experimentsDF, tests, instances)
} else {
  #Option 2: download the files from s3 and process them.
  df <- processExperimentsS3(dirName, df, experimentsDF, tests, instances)
}

#Group and aggregate the generated dataframe to derive metrics for each experiment
#and each test considering the various instances.
df <- df %>%
  group_by(EXPERIMENT, TEST, LABEL, SIZE, NODES, SYSTEM) %>%
  summarize(AVG_TOTAL_DURATION_HOUR = mean(TOTAL_DURATION_HOUR, na.rm = TRUE),
            AVG_DURATION_SEC = mean(AVERAGE_DURATION_SEC, na.rm = TRUE),
            GEOMEAN_DURATION_SEC = mean(GEOMEAN_DURATION_SEC, na.rm = TRUE))

#Save the dataframe to an excel file.
outXlsxFile <- file.path(prefixOS, "Documents/stacked_bar_chart.xlsx")
export(df, outXlsxFile)

#Generate the stacked chart. The total is computed within the function that generates the plot.
metric <- "AVG_TOTAL_DURATION_HOUR"
plot <- createStackedChartFromDF(df, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB")
outPngFile <- file.path(prefixOS, "Documents/stacked_bar_chart.png")
png(outPngFile, width=1500, height=500, res=120)
print(plot)
dev.off()

#Group and aggregate the generated dataframe to derive metrics for each experiment
#for the full benchmark considering the various aggregate metrics for the tests.
df <- df %>%
  group_by(EXPERIMENT, LABEL, SIZE, NODES, SYSTEM) %>%
  summarize(TOTAL_DURATION_HOUR = sum(AVG_TOTAL_DURATION_HOUR, na.rm = TRUE))

#Save the dataframe to an excel file.
outXlsxFile <- file.path(prefixOS, "Documents/elasticity_chart.xlsx")
export(df, outXlsxFile)

#Generate the elasticity curve.
metric <- "TOTAL_DURATION_HOUR"
df$NODES <- as.numeric(df$NODES)
plot <- createElasticityPlotFromDF(df, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB", FALSE)
outPngFile <- file.path(prefixOS, "Documents/elasticity_chart.png")
png(outPngFile, width=800, height=800, res=120)
print(plot)
dev.off()

#Compute the speedup with the dataframe containing the total duration for the benchmark.
#First, get a table with the Databricks experiments, and a table with the snowflake experiments.
dfDBR <- df[df$SYSTEM == "DBR_DLT_NOZ_PART",]
dfSFK <- df[df$SYSTEM == "SNOWFLAKE",]
#Join the two tables on SIZE (columns are renamed with x and y suffixes: DBR=x, SFK=y)
dfJoin <- inner_join(dfDBR, dfSFK, by="SIZE")
#Add a column with the speedup.
dfJoin$SPEEDUP <- dfJoin$TOTAL_DURATION_HOUR.x / dfJoin$TOTAL_DURATION_HOUR.y

#Save the dataframe to an excel file.
outXlsxFile <- file.path(prefixOS, "Documents/speedup.xlsx")
export(dfJoin, outXlsxFile)

#Generate the speedup curve.
metric <- "SPEEDUP"
dfJoin$NODES.x <- as.numeric(dfJoin$NODES.x)
plot <- createSpeedupPlotFromDF(dfJoin, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB", FALSE)
outPngFile <- file.path(prefixOS, "Documents/speedup.png")
png(outPngFile, width=800, height=800, res=120)
print(plot)
dev.off()

