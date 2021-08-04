library(openxlsx)
library(ggplot2)
library("rio")

path <- "C:/Users/bscuser/git/tpcdsbench/analysis/Scripts"

metricsDF <- NULL
systemsDF <- NULL
testsDF <- NULL
outFile <- NULL

metricsDF = read.xlsx(file.path(path, "../FileStats/FileStats.xlsx"), sheet="SummaryNumFiles", colNames=FALSE, rows=seq(2,10), cols=seq(2,7))
systemsDF = read.xlsx(file.path(path, "../FileStats/FileStats.xlsx"), sheet="SummaryNumFiles", colNames=FALSE, rows=seq(1,1), cols=seq(2,7))
testsDF = read.xlsx(file.path(path, "../FileStats/FileStats.xlsx"), sheet="SummaryNumFiles", colNames=FALSE, rows=seq(2,10), cols=seq(1,1))
outFile <- file.path(path, "../FileStats/num_files.png")

#print(metricsDF)
#print(systemsDF)
#print(testsDF)

testsList <- split(testsDF, seq(nrow(testsDF)))
systemsList <- as.list(systemsDF)

nrows <- length(testsList)
ncols <- length(systemsList)

tableDF <- data.frame(
  SYSTEM=character(),
  TEST=character(),
  NUM_FILES=numeric(),
  stringsAsFactors=FALSE)

for(i in 1:nrows) {
  for(j in 1:ncols) {
    #cat(paste0(metricsDF[i,j], " "))
    tableDF[nrow(tableDF) + 1,] = c(systemsList[j], testsList[i], metricsDF[i,j])
  }
  #cat("\n")
}

outXlsxFile <- file.path(path, "../FileStats/tableNumFiles.xlsx")
export(tableDF, outXlsxFile)

tableDF$SYSTEM <- factor(tableDF$SYSTEM)
tableDF$TEST <- factor(as.character(tableDF$TEST), levels = c("load", "upsert", "compact 1", "delete 0.1%", "delete 1%", "delete 10%", "compact 2", "GDPR", "compact 3"))
tableDF$NUM_FILES <- as.numeric(tableDF$NUM_FILES)

print(tableDF)

dataframeColsTypes <- sapply(tableDF, class)
print(dataframeColsTypes)

#Create the plot with faceting from the built dataframe
plot <- ggplot(data=tableDF, aes(TEST, NUM_FILES)) + 
  geom_col(fill="cornflowerblue") +
  geom_text(aes(label = NUM_FILES), vjust = -0.2, size = 4) +
  facet_wrap(~ SYSTEM, ncol=1) +
  ylab("Number of files") +
  ylim(0, 220000) +
  theme(axis.title.x=element_blank()) +
  theme(axis.text=element_text(size=14), axis.title=element_text(size=14)) +
  theme(legend.text=element_text(size=14)) +
  theme(strip.text = element_text(size = 14))

png(outFile, width=1200, height=900, res=120)
print(plot)
dev.off()



