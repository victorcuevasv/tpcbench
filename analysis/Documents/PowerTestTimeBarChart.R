library(openxlsx)
library(ggplot2)
#Use the stringr package to use the str_wrap function below
library(stringr)

#inFile <- "./Documents/RESULTS/power/summaryPower.xlsx"
#outFile <- "./Documents/RESULTS/power/PowerTestTimeBarChart.pdf"

inFile <- "./Documents/RESULTS/load/summaryLoad.xlsx"
outFile <- "./Documents/RESULTS/load/LoadTestTimeBarChart.pdf"

dataf = read.xlsx(inFile, sheet="Sheet 1", rows=seq(1,3), cols=seq(1,4))

# Map DisplayNames to pseudonyms
pseudonyms<-new.env()
#pseudonyms[["presto"]]<-"Presto"
pseudonyms[["presto"]]<-"HadoopMR"
pseudonyms[["spark"]]<-"Spark"
i <- 1
for(name in dataf$SYSTEM) {
  dataf[i,]$SYSTEM <- pseudonyms[[name]]
  i <- i + 1
}

print(dataf)

#Change SYSTEM to a factor to present the bars in the order in which they appear in the file
dataf$SYSTEM <- factor(dataf$SYSTEM, levels=dataf$SYSTEM)
pdf(file=outFile, width=7, height=4)
ggplot(data=dataf, aes(x=SYSTEM, y=TOTAL_DURATION, label=round(TOTAL_DURATION, digits=0))) + 
#The dodge position makes the bars appear side by side
geom_bar(stat="identity", position=position_dodge()) + 
#This line adds the exact values on top of the bars
geom_text(size = 6, position=position_dodge(width=1), vjust=0) +
theme(axis.title.x=element_blank()) + 
theme(axis.text=element_text(size=16), axis.title=element_text(size=18)) +
#The str_wrap function makes the name of the column appear on multiple lines instead of just one
scale_x_discrete(labels = function(x) str_wrap(x, width = 7)) + 
scale_y_continuous("Time (sec.)", limits=c(0,1200)) + 
scale_fill_manual(name="", values=c("#585574")) + 
theme(legend.position = "bottom") + 
theme(legend.text=element_text(size=16)) 
dev.off()


