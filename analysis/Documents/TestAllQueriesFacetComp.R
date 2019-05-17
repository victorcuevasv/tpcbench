library("rio")
library(ggplot2)
library(dplyr)
#Use the stringr package to use the str_wrap function below
library(stringr)

#inFile <- "./Documents/RESULTS/power/merged_power.xlsx"
#outFile <- "./Documents/RESULTS/power/PowerTestFacetComp.pdf"

inFile <- "./Documents/RESULTS/load/merged_load.xlsx"
outFile <- "./Documents/RESULTS/load/LoadTestFacetComp.pdf"

#Read the data.
dataf = import(inFile)
#List that represents all queries
queriesAll <- seq(1, 99)
#queriesAll <- seq(1, 25)
#Filter the queries below if desired
queriesRemove <- seq(31, 99)
queries <- setdiff(queriesAll, queriesRemove)
dataf <- dataf[ dataf$QUERY %in% queries, ]
#Convert the query into a factor
dataf$QUERY <- factor(dataf$QUERY, levels=queriesAll)

#Generate a print a summary of execution times for all systems.
dataf <- dataf %>% group_by(SYSTEM) %>% mutate(SYSTEM_AVERAGE = mean(DURATION))
print(dataf %>% select(SYSTEM, SYSTEM_AVERAGE) %>% distinct)
#print(dataf)

#Generate the faceted graph.
pdf(file=outFile, width=7, height=4)
#Use the factor function to transform the ScaleFactor column from continuous to categorical
ggplot(data=dataf, aes(x=QUERY, y=DURATION, fill=factor(SYSTEM))) +  
#The dodge position makes the bars appear side by side
geom_bar(stat="identity", position=position_dodge()) +  
#facet_wrap(~ SYSTEM, ncol=2) +
#geom_hline(aes(yintercept=SYSTEM_AVERAGE), data=dataf) +
ylab("Exec. time (sec.)") +
theme(axis.title.x=element_blank()) + 
theme(axis.text=element_text(size=14), axis.title=element_text(size=18)) +
#The str_wrap function makes the name of the column appear on multiple lines instead of just one
scale_x_discrete(labels = function(x) str_wrap(x, width = 10)) + 
scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("EMR Presto", "Databricks Spark")) + 
#scale_fill_manual(name="", values=c("#585574", "#DDD4B3"), labels=c("1 TB", "10 TB")) + 
#This line adds the exact values on top of the bars
#geom_text(aes(label=TPTSQL), position=position_dodge(width=0.9), vjust=-0.25)
theme(legend.title = element_blank()) +
theme(legend.position = c(0.175, 0.85)) +
#theme(legend.position = c(0.825, 0.875)) +
theme(strip.text.x=element_text(size=18)) +
theme(legend.text=element_text(size=14)) 
dev.off()



