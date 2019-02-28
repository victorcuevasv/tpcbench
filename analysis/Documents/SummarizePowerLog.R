library("rio")

inFile <- "./Documents/RESULTS/power/spark/analytics.log"
outFile <- "./Documents/RESULTS/power/spark/summary.xlsx"

#Add colClasses="character" to read the file as characters.
#Otherwise, long numbers will not be read correctly.
analytics <- import(inFile, format="psv", colClasses="character")

#Convert the character colums to numeric values where needed.
analytics$STARTDATE_EPOCH <- as.numeric(analytics$STARTDATE_EPOCH)
analytics$STOPDATE_EPOCH <- as.numeric(analytics$STOPDATE_EPOCH)
analytics$DURATION_MS <- as.numeric(analytics$DURATION_MS)

totalDurationMSEC <- max(analytics$STOPDATE_EPOCH) - min(analytics$STARTDATE_EPOCH)
totalDurationSEC <- totalDurationMSEC / 1000
avgDurationSEC = mean(analytics$DURATION_MS) / 1000

#Create a new dataframe to hold the aggregate results.

outputDF <- data.frame(TOTAL_DURATION=double(),
                       AVG_QUERY_DURATION=double(),
                      stringsAsFactors=FALSE)

outputDF[nrow(outputDF) + 1,] = list(totalDurationSEC, avgDurationSEC)

export(outputDF, outFile)




