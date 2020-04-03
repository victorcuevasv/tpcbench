options(repr.plot.width=1500, repr.plot.height=400)

library("aws.s3")
library("rio")
library("ggplot2")
library("stringr")
library("dplyr")

Sys.setenv("AWS_ACCESS_KEY_ID" = "",
           "AWS_SECRET_ACCESS_KEY" = "",
           "AWS_DEFAULT_REGION" = "us-west-2")

saveS3ObjectToFile <- function(s3objURL, outFile) {
  s3obj <- get_object(s3objURL)
  charObj <- rawToChar(s3obj)
  sink(outFile)
  cat (charObj)
  sink()
}

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
  dataframe[nrow(dataframe) + 1,] = list(label, test, instance, totalDurationSec, totalDurationHour)
  return(dataframe)
}

createPlotFromDataframe <- function(dataf, metric, metricsLabel, metricsUnit, metricsDigits, title){
  ggplot(data=dataf, aes(x=SYSTEM, y=get(metric), fill=TEST), width=7, height=7) + 
    geom_bar(stat="identity", position = position_stack(reverse = T)) + 
    #It may be necessary to use fun.y in some environments.
    #geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = SYSTEM), stat = 'summary', fun.y = sum, vjust = -1, size=6) +     
    geom_text(aes(label = round(stat(y), digits=metricsDigits[[metric]]), group = SYSTEM), stat = 'summary', fun = sum, vjust = -1, size=6) +    
    theme(axis.title.x=element_blank()) + 
    theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
    #The str_wrap function makes the name of the column appear on multiple lines instead of just one
    scale_x_discrete(labels = function(x) str_wrap(x, width = 30)) + 
    scale_y_continuous(paste0(metricsLabel[[metric]], " ", " (", metricsUnit[[metric]], ")"), limits=c(0,metricsYaxisLimit[[metric]])) + 
    #Use the line below to specify the colors manually -must provide enough colors-
    scale_fill_manual(name="", values=c("#5e3c99", "#b2abd2", "#fdb863", "#e66101"), labels=c('load', 'analyze', 'power', 'tput')) + 
    theme(legend.position = "bottom") + 
    theme(legend.text=element_text(size=14)) + 
    theme(plot.margin=margin(t = 10, r = 5, b = 5, l = 5, unit = "pt"))
}

processExperiments <- function(dataframe, experiments, labels, tests, instances) {
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

# Map metrics to descriptions for the y-axis.
metricsLabel<-new.env()
metricsLabel[["AVG_TOTAL_DURATION_HOUR"]] <- "Total"
metricsUnit<-new.env()
metricsUnit[["AVG_TOTAL_DURATION_HOUR"]] <- "hr."
metricsDigits<-new.env()
metricsDigits[["AVG_TOTAL_DURATION_HOUR"]] <- 2
metricsYaxisLimit<-new.env()
metricsYaxisLimit[["AVG_TOTAL_DURATION_HOUR"]] <- 50.0

metric <- "AVG_TOTAL_DURATION_HOUR"

s3Prefix <- "s3://presto-comp/analytics/"
#Create a new dataframe to hold the aggregate results.
df <- data.frame(SYSTEM=character(),
                       TEST=character(),
                       INSTANCE=character(),
                       TOTAL_DURATION_SEC=double(),
                       TOTAL_DURATION_HOUR=double(),
                       stringsAsFactors=FALSE)

experiments <- list('prestoemr-529-500bucket', 'prestoemr-529-8bucket', 'prestoemr-529', 'prestoemr-529part', 'prestoemr220',
                    'prestoemr220nostats', 'prestoemr220part')
labels <- list('5.29 500 buck', '5.29 8 buck', '5.29', '5.29 part', '5.26', '5.26 no stats', '5.26 part' )
tests <- list('analyze', 'load', 'power', 'tput')
instances <- list('1', '2', '3')

df <- processExperiments(df, experiments, labels, tests, instances)

df <- df %>%
  group_by(SYSTEM, TEST) %>%
  summarize(AVG_TOTAL_DURATION_HOUR = mean(TOTAL_DURATION_HOUR, na.rm = TRUE))

#print(df, n=nrow(df))

createPlotFromDataframe(df, metric, metricsLabel, metricsUnit, metricsDigits, "TPC-DS Full Benchmark at 1 TB")

