import os
import re

workDir = "C:\\Users\\User\\tpcds_queries1GB\\QueriesSpark"

def listFiles(workDir) :
    resList = []
    for path, dirs, files in os.walk(workDir):
        resList = list(map(lambda f: os.path.join(path, f), files))
    return resList

def generateScalaMap(fileList, outFileName) :
    out = open(outFileName, "w")
    out.write("class TPCDS_Schemas { \n")
    out.write("\n   val tpcdsSchemasMap = Map( ")
    queriesList = []
    for file in fileList :
        # Get filename without path
        fileName = os.path.basename(file)
        print(f'Processing file: {fileName}')
        with open(file, 'r') as fileptr:
            contents = fileptr.read()
            queriesList.append(f'\"{fileName.rstrip(".sql")}\" -> \"\"\"{contents}\"\"\"')
    allQueries = " , \n".join(queriesList)
    out.write(allQueries)
    out.write(") }")
    out.close()

fileList = listFiles(workDir)
fileList = list(filter(lambda x : x.endswith(".sql"), fileList))
fileList = sorted(fileList, key = lambda x : int(os.path.basename(x).lstrip("query").rstrip(".sql")))
scalaMap = generateScalaMap(fileList, "TPCDS_Queries1GB.scala")
print(scalaMap)

