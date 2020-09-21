library("rio")
library(ggplot2)
library(dplyr)
library(gridExtra)
#Use the stringr package to use the str_wrap function below
library(stringr)

#inFile <- "./Documents/RESULTS/load/merged_load.xlsx"
#outDir <- "./Documents/RESULTS/load"

inFile <- "./Documents/RESULTS/power/merged_power.xlsx"
outDir <- "./Documents/RESULTS/power"

#List that represents all of the queries.
queriesAll <- seq(1, 99)
#queriesAll <- seq(1, 25)
#List of queries to filter.
#queriesRemove <- seq(31, 99)
queriesRemove <- c()
#Number of queries to include in each graph.
#nQueries <- 12
nQueries <- 20

#Read the data.
datafAll = import(inFile)
#List that will contain all the generated plots.
plots <- list()
#Apply the filter for queries.
queries <- setdiff(queriesAll, queriesRemove)
#Variable to index the plots
nPlot <- 1

while( TRUE ) {
  #Create a vector containing the up to nQueries queries to show in each plot.
  queriesPlot <- NULL
  for( i in 1:nQueries ) {
    if( length(queries) > 0 ) {
      queriesPlot <- c(queriesPlot, queries[1] )
      queries <- setdiff(queries, queries[1])
    }
  }
  if( length(queriesPlot) == 0 )
    break
  else {
    print(queriesPlot)
  }
  dataf <- datafAll[ datafAll$QUERY %in% queriesPlot, ]
  #Convert the query into a factor
  dataf$QUERY <- factor(dataf$QUERY, levels=queriesAll)
  #Generate the plot and add it to the list.
  p <- ggplot(data=dataf, aes(x=QUERY, y=DURATION, fill=factor(SYSTEM))) +  
  #The dodge position makes the bars appear side by side
  geom_bar(stat="identity", position=position_dodge()) +  
  #facet_wrap(~ SYSTEM, ncol=2) +
  #geom_hline(aes(yintercept=SYSTEM_AVERAGE), data=dataf) +
  ylab("Exec. time (sec.)") +
  theme(axis.title.x=element_blank()) + 
  theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
  #The str_wrap function makes the name of the column appear on multiple lines instead of just one
  scale_x_discrete(labels = function(x) str_wrap(x, width = 8)) +
  scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("EMR Presto", "Databricks Spark")) + 
  scale_y_continuous(limits=c(0, 3000)) +
  #scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("1 TB", "10 TB")) + 
  #This line adds the exact values on top of the bars
  #geom_text(aes(label=TPTSQL), position=position_dodge(width=0.9), vjust=-0.25)
  theme(legend.title = element_blank()) +
  theme(legend.position = c(0.175, 0.875)) +
  #theme(legend.position = c(0.825, 0.875)) +
  theme(strip.text.x=element_text(size=18)) +
  theme(legend.text=element_text(size=12)) 
  plots[[nPlot]] <- p
  names(plots)[nPlot] <- paste(queriesPlot[1], "-", queriesPlot[length(queriesPlot)])
  nPlot <- nPlot + 1
}

#Save one plot per file
#invisible(mapply(ggsave, file=paste0(outDir, "/", names(plots), ".pdf"), plot=plots, width=7, height=4))

#Save all of the plots in a single file, arrange them all in a single page
#must specify the appropriate total size
#ggsave(paste0(outDir, "/", "PowerTestCompAll.pdf"), width=7, height=16, arrangeGrob(grobs = plots, ncol=1))

#Save all of the plots in a single file, arrange them all in 2 x 1 plot pages
#ggsave(paste0(outDir, "/", "PowerTestCompAll.pdf"), width=7, height=4, marrangeGrob(grobs=plots, nrow=1, ncol=2))

#Save all of the plots in a single file, one plot per page
pdf(paste0(outDir, "/", "PowerTestCompAll.pdf"), width=7, height=4)
invisible(lapply(plots, print))
dev.off()




