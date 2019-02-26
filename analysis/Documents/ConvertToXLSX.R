library("rio")

analytics <- import("./Documents/RESULTS/presto_power/analytics.log", format="psv")

export(analytics, "./Documents/RESULTS/presto_power/analytics.xlsx")

