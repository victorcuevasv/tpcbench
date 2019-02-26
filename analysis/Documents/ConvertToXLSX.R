library("rio")

analytics <- import("./Documents/RESULTS/presto_power/analytics.psv")

export(analytics, "./Documents/RESULTS/presto_power/analytics.xlsx")

#list.files("./Documents")