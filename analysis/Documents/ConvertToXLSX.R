library("rio")

workDir <- "./Documents/RESULTS/power"
systemDirs <- list.files(path=workDir)

#It is assumed that there is a single .log file with the run data.
for(system in systemDirs) {
  systemDir <- paste(workDir, "/", system, sep="")
  files <- list.files(path=systemDir, pattern = "\\.log$")
  inFile <- paste(workDir, "/", system, "/", files[1], sep="")
  outFileName <- paste(tools::file_path_sans_ext(files[1]), ".xlsx", sep="")
  outFile <- paste(workDir, "/", system, "/", outFileName, sep="")
  analytics <- import(inFile, format="psv")
  export(analytics, outFile)
}


