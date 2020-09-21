#!/usr/bin/env Rscript
#args[1] csv file with experiment data

#Use for running locally
prefixOS <- getwd()
#Change if running in Docker
if( prefixOS == "/" )
  prefixOS <- "/home/rstudio"

#Next line with INSTALL_opts may be necessary on windows.
#install.packages("aws.s3", repos = c("cloudyr" = "https://cloud.R-project.org"), INSTALL_opts = "--no-multiarch")
library("aws.s3")
library("rio")
library("ggplot2")
library("stringr")
library("dplyr")
library("EnvStats")

processDatabricksFile <- function(inFile, dataframe, test) {
  analytics <- import(inFile, format="csv", colClasses="character")
  analytics <- analytics %>%  rename(EXPERIMENT=runId, LABEL=runLabel, INSTANCE=iteration,
                                     QUERY=queryName, DURATION_MS=timeMs)
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
      if ( dataframeCol == "TEST" )
        analytics[dataframeCol] <- test
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

createBarChartFromDF <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title, 
                                 factorValuesList, i){
  metricsYaxisLimit[[metric]] <- 250
  xAxisObj <- element_text(size=16)
  if( i %% 2 == 0) {
    title <- ""
    #xAxisObj <- element_blank()
  }
  #Use the factor function to transform the ScaleFactor column from continuous to categorical
  ggplot(data=dataf, aes(x=factor(QUERY_NAME, levels=rev(factorValuesList)), y=get(metric), fill=factor(LABEL))) +  
    #ggplot(data=dataf, aes(x=factor(QUERY), y=DURATION, label=round(DURATION, digits=metricsDigits[[metric]]))) +
    #The dodge position makes the bars appear side by side
    ggtitle(title) +
    theme(plot.title = element_text(size=18, face="bold")) +
    geom_bar(stat="identity", position=position_dodge()) +
    coord_flip() +
    geom_text(aes(label = round(stat(y), digits=1)), hjust = -0.15, size=5) + 
    #This line adds the exact values on top of the bars
    #geom_text(size = 5, position=position_dodge(width=1), vjust=0.1) +
    #facet_wrap(~ SYSTEM, ncol=2) +
    #geom_hline(aes(yintercept=SYSTEM_AVERAGE), data=dataf) +
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), 
                       limits=c(0,metricsYaxisLimit[[metric]])) + 
    theme(axis.title.x=xAxisObj) + 
    theme(axis.text=element_text(size=16), axis.title.y=element_blank()) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    #scale_x_discrete(labels = function(x) str_wrap(x, width = 10)) + 
    #scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("EMR Presto", "Databricks Spark")) + 
    #This line adds the exact values on top of the bars
    #geom_text(aes(label=TPTSQL), position=position_dodge(width=0.9), vjust=-0.25)
    theme(legend.title = element_blank()) +
    #theme(legend.position = c(0.175, 0.85)) +
    theme(strip.text.x=element_text(size=18)) +
    theme(legend.text=element_text(size=14)) +
    scale_fill_manual(values=c("darkseagreen")) +
    #theme(legend.position="bottom")
    theme(legend.position="none")
}

createBarChartMaxMinFromDF <- function(dataf, metric, metricMax, metricMin, metricsLabel, metricsUnit, 
                                       metricsDigits, title, factorValuesList, i){
  metricsYaxisLimit[[metric]] <- 250
  xAxisObj <- element_text(size=16)
  if( i %% 2 == 0) {
    title <- ""
    #xAxisObj <- element_blank()
  }
  ggplot(data=dataf, aes(x=factor(QUERY_NAME, levels=rev(factorValuesList)), y=get(metric), ymin=get(metricMin), 
                         ymax=get(metricMax), fill=factor(LABEL))) +
    ggtitle(title) + theme(plot.title = element_text(size=18, face="bold")) +
    geom_bar(stat="identity", position=position_dodge()) + 
    geom_errorbar(stat="identity", position=position_dodge(width = 0.9), width=0.3) +
    coord_flip() +
    #Add the values on top of the bars.
    geom_text(aes(label = round(stat(ymax), digits=1), y = round(stat(ymax), digits=1)), hjust = -0.15, size=5) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), 
                       limits=c(0,metricsYaxisLimit[[metric]])) + 
    #theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
    theme(axis.text=element_text(size=16), axis.title.y=element_blank()) +
    theme(axis.title.x = xAxisObj) +
    theme(legend.title = element_blank()) +
    theme(strip.text.x=element_text(size=16)) +
    theme(legend.text=element_text(size=14)) +
    scale_fill_manual(values=c("antiquewhite3")) +
    #theme(legend.position="bottom")
    theme(legend.position="none")
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

extractQueryNumber <- function(queryName) {
  has_b <- grepl("b", queryName, fixed=TRUE)
  queryNumStr <- gsub("q", "", queryName)
  queryNumStr <- gsub("a", "", queryNumStr)
  queryNumStr <- gsub("b", "", queryNumStr)
  queryNum <- as.numeric(queryNumStr)
  if( has_b == TRUE )
    queryNum <- queryNum + 0.5
  return (queryNum)
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

#Option 1: use all of the subdirectories found in the directory.
#For the labels, use the subdirectory name.
if( length(args) < 1 ) {
  print("Usage: queriesDatabricks.R <csv file>")
} else {
  fileName <- file.path(args[1])
}
tests <- list('power')

#Create a new dataframe to hold the aggregate results, pass it to the various functions.
df <- data.frame(
                 EXPERIMENT=character(),
                 LABEL=character(),
                 TEST=character(),
                 INSTANCE=numeric(),
                 QUERY=character(),
                 STREAM=numeric(),
                 ITEM=numeric(),
                 STARTDATE_EPOCH=numeric(),
                 STOPDATE_EPOCH=numeric(),
                 DURATION_MS=numeric(),
                 DURATION=numeric(),
                 RESULTS_SIZE=numeric(),
                 TUPLES=numeric(),
                 stringsAsFactors=FALSE)

df <- processDatabricksFile(fileName, df, "power")

#Compute the duration.
df$DURATION <- df$DURATION_MS / 1000

#Compute the ITEM, in case it was not specified in the data.
#df <- df %>% group_by(EXPERIMENT, LABEL, TEST, INSTANCE, STREAM) %>%
#  mutate(ITEM=with_order(order_by=STARTDATE_EPOCH, fun=row_number, x=STARTDATE_EPOCH))

#Compute the QUERY_RANK, i.e., the rank of the query in the test according to execution time.
df <- df %>% group_by(EXPERIMENT, LABEL, TEST, QUERY) %>%
  mutate(QUERY_RANK=with_order(order_by=DURATION, fun=row_number, x=DURATION))

#To generate the numeric query identifiers, first rename the QUERY column to QUERY_NAME.
df <- df %>%  rename(QUERY_NAME=QUERY)

#Now generate a new QUERY column by converting the query names to numbers only, in order
#to ensure that they can be ordered as expected. Those numbers can include decimals like 14.5
#for the queries that are separated in two parts.
df$QUERY <- mapply(extractQueryNumber, df$QUERY_NAME)

#Update the QUERY to the order within the list of queries.
df <- df %>% group_by(EXPERIMENT, LABEL, TEST, INSTANCE) %>%
  mutate(QUERY=with_order(order_by=QUERY, fun=row_number, x=QUERY))

outXlsxFile <- file.path(prefixOS, "Documents/rawdataDBR.xlsx")
export(df, outXlsxFile)

dfRank1 <- df %>% filter(QUERY_RANK == 1) %>% arrange(QUERY)

outXlsxFile <- file.path(prefixOS, "Documents/rawdataDBRrank1.xlsx")
export(dfRank1, outXlsxFile)

#Eliminate the beginning q in the query names.
df$QUERY_NAME <- gsub("q", "", df$QUERY_NAME)
#Obtain a vector with the query names to use in the charts.
factorValuesDF <- df %>% filter(INSTANCE == 1) %>% arrange(QUERY)
factorValuesList <- as.vector(factorValuesDF$QUERY_NAME)

#Use only the rank 1 queries.
#df <- df %>% filter(QUERY_RANK == 1)

df <- df %>%
  group_by(EXPERIMENT, LABEL, TEST, QUERY, QUERY_NAME) %>%
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

outXlsxFile <- file.path(prefixOS, "Documents/experimentsDBR.xlsx")
export(df, outXlsxFile)

metric <- "AVG_DURATION_SEC"
metricMax <- "MAX_DURATION_SEC"
metricMin <- "MIN_DURATION_SEC"
yAxisDefaultLimit <- metricsYaxisLimit[[metric]]
for( test in tests  ) {
  title <- paste0("TPC-DS ", test, " test at 30 TB")
  dfFiltered <- df[df$TEST == test,]
  #queriesPerChart <- 21
  queriesPerChart <- 26
  iterations <- ceiling(n_distinct(dfFiltered$QUERY) / queriesPerChart)
  plotMaxMin <- TRUE
  #plotMaxMin <- FALSE
  for(i in 1:iterations) {
    lower <- (i-1) * queriesPerChart + 1
    upper <- i * queriesPerChart
    dfSubset <- dfFiltered[dfFiltered$QUERY >= lower & dfFiltered$QUERY <= upper,]
    if( ! plotMaxMin ) {
      metricsYaxisLimit[[metric]] <- getRoundedLimit(max(dfFiltered[[metric]]))
      plot <- createBarChartFromDF(dfSubset, metric, metricsLabel, metricsUnit, metricsDigits,
                                   title, factorValuesList, i)
    } else {
      metricsYaxisLimit[[metric]] <- getRoundedLimit(max(dfFiltered[[metricMax]]))
      plot <- createBarChartMaxMinFromDF(dfSubset, metric, metricMax, metricMin, metricsLabel,
                                         metricsUnit, metricsDigits, title, factorValuesList, i)
    }
    outPngFile <- file.path(prefixOS, paste0("Documents/bar_chart_", test, "_", i, ".png"))
    png(outPngFile, width=700, height=1000, res=120)
    print(plot)
    dev.off()
  }
}
metricsYaxisLimit[[metric]] <- yAxisDefaultLimit


