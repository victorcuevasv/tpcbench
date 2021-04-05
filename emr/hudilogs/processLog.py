import os
import sys
import json

experiment = 'tpcds-warehouse-sparkemr-532-1000gb-1-1616980602huditest'

def processFile(data, output, experiment, step) :
    jsonObj = json.load(data)["partitionToWriteStats"]
    #Process each element of the form "ss_sold_date_sk=2452609"
    for array in jsonObj.values() :
        #Process each element within the array of the outside object
        for element in array :
            processElement(element, output, experiment, step)


def processElement(element, output, experiment, step) :
    fileId = element["fileId"]
    path = element["path"]
    numWrites = element["numWrites"]
    numDeletes = element["numDeletes"]
    numUpdateWrites = element["numUpdateWrites"]
    numInserts = element["numInserts"]
    totalWriteBytes = element["totalWriteBytes"]
    partitionPathFull = element["partitionPath"]
    #Obtain the partition value from the value like "ss_sold_date_sk=2452609"
    partitionPath = partitionPathFull[partitionPathFull.find('=') + 1 : ]
    fileSizeInBytes = element["fileSizeInBytes"]
    output.write(experiment + "|" + str(step) + "|" + fileId + "|" + path + "|" + str(numWrites) 
                 + "|" + str(numDeletes) + "|" + str(numUpdateWrites) + "|" + str(numInserts) +
                 "|" + str(totalWriteBytes) + "|" + partitionPath + "|" + str(fileSizeInBytes) + "\n")


output = open("hudiLog.csv", "w")
output.write("experiment|step|fileId|path|numWrites|numDeletes|numUpdateWrites|numInserts" +
             "|totalWriteBytes|partitionPath|fileSizeInBytes\n")
step = 0
for file in os.listdir(experiment):
    current = os.path.join(experiment, file)
    if os.path.isfile(current) :
        data = open(current, "r")
        processFile(data, output, experiment, step)
    step += 1
output.close()





