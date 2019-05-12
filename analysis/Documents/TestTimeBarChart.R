library(openxlsx)
library(ggplot2)
#Use the stringr package to use the str_wrap function below
library(stringr)

#######################################################
#testDone <- "load"
#testDone <- "power"
testDone <- "tput"
#metric <- "TOTAL_DURATION_SEC"
metric <- "TOTAL_DURATION_HOUR"
#metric <- "AVG_QUERY_DURATION_SEC"
#metric <- "GEOM_MEAN_QUERY_DURATION_SEC"
topLimit <- 30000 #Only used if line 51 is modified
#######################################################

# Map metrics to descriptions for the y-axis.
metricsLabel<-new.env()
metricsLabel[["TOTAL_DURATION_HOUR"]]<-"Total"
metricsLabel[["TOTAL_DURATION_SEC"]]<-"Total"
metricsLabel[["AVG_QUERY_DURATION_SEC"]]<-"Average query"
metricsLabel[["GEOM_MEAN_QUERY_DURATION_SEC"]]<-"Geom. mean query"

metricsUnit<-new.env()
metricsUnit[["TOTAL_DURATION_HOUR"]]<-"hr."
metricsUnit[["TOTAL_DURATION_SEC"]]<-"sec."
metricsUnit[["AVG_QUERY_DURATION_SEC"]]<-"sec."
metricsUnit[["GEOM_MEAN_QUERY_DURATION_SEC"]]<-"sec."

metricsDigits<-new.env()
metricsDigits[["TOTAL_DURATION_HOUR"]]<-2
metricsDigits[["TOTAL_DURATION_SEC"]]<-0
metricsDigits[["AVG_QUERY_DURATION_SEC"]]<-0
metricsDigits[["GEOM_MEAN_QUERY_DURATION_SEC"]]<-0

metricsPrefix<-new.env()
metricsPrefix[["TOTAL_DURATION_HOUR"]]<-"totalHr"
metricsPrefix[["TOTAL_DURATION_SEC"]]<-"totalSec"
metricsPrefix[["AVG_QUERY_DURATION_SEC"]]<-"avg"
metricsPrefix[["GEOM_MEAN_QUERY_DURATION_SEC"]]<-"geomean"

inFile <- paste0("./Documents/RESULTS/", testDone, "/summary_", testDone, ".xlsx")
outFile <- paste0("./Documents/RESULTS/", testDone, "/", testDone, "_", metricsPrefix[[metric]], "TimeBarChart.pdf")

dataf = read.xlsx(inFile, sheet="Sheet 1", rows=seq(1,3), cols=seq(1,7))

# Map DisplayNames to pseudonyms
pseudonyms<-new.env()
pseudonyms[["prestoemr"]]<-"Presto"
pseudonyms[["sparkdatabricks"]]<-"Spark"
i <- 1
for(name in dataf$SYSTEM) {
  dataf[i,]$SYSTEM <- pseudonyms[[name]]
  i <- i + 1
}

print(dataf)

#Change SYSTEM to a factor to present the bars in the order in which they appear in the file
dataf$SYSTEM <- factor(dataf$SYSTEM, levels=dataf$SYSTEM)
pdf(file=outFile, width=7, height=4)
ggplot(data=dataf, aes(x=SYSTEM, y=get(metric), label=round(get(metric), digits=metricsDigits[[metric]]))) + 
#The dodge position makes the bars appear side by side
geom_bar(stat="identity", position=position_dodge()) + 
#This line adds the exact values on top of the bars
geom_text(size = 6, position=position_dodge(width=1), vjust=0) +
theme(axis.title.x=element_blank()) + 
theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
#The str_wrap function makes the name of the column appear on multiple lines instead of just one
scale_x_discrete(labels = function(x) str_wrap(x, width = 7)) + 
scale_y_continuous(paste0(metricsLabel[[metric]], " ", "time (", metricsUnit[[metric]], ")")) + 
#Use the line below to specify the limit for the y axis
#scale_y_continuous(paste0(metricsLabel[[metric]], " ", "time (sec.)"), limits=c(0,topLimit)) + 
scale_fill_manual(name="", values=c("#585574")) + 
theme(legend.position = "bottom") + 
theme(legend.text=element_text(size=16)) +
theme(plot.margin=margin(t = 5, r = 5, b = 5, l = 5, unit = "pt"))
dev.off()


