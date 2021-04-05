import os
import sys
import json

experiment = 'tpcds-warehouse-sparkemr-620-1000gb-1-1616932275icebtest'   
file = '00005-74fe28ef-ab3d-4d75-843a-d678ad27d4ee.metadata.json'

def processFile(data, output, experiment) :
    array = json.load(data)["snapshots"]
    #Process each summary element.
    step = 0
    for element in array :
        processElement(element["summary"], output, experiment, step)
        step += 1


def processElement(element, output, experiment, step) :
    addedDataFiles = element.get('added-data-files', '')
    deletedDataFiles = element.get('deleted-data-files', '')
    addedRecords = element.get('added-records', '')
    deletedRecords = element.get('deleted-records', '')
    addedFilesSize = element.get('added-files-size', '')
    removedFilesSize = element.get('removed-files-size', '')
    changedPartitionCount = element.get('changed-partition-count', '')
    totalRecords = element.get('total-records', '')
    totalDataFiles = element.get('total-data-files', '')
    totalDeleteFiles = element.get('total-delete-files', '')
    output.write(experiment + "|" + str(step) + "|" + str(addedDataFiles) + "|" + str(deletedDataFiles)
                 + "|" + str(addedRecords) + "|" + str(deletedRecords) + "|" + str(addedFilesSize) + "|" 
                 + str(removedFilesSize) + "|" + str(changedPartitionCount) + "|" + str(totalRecords) + "|"
                 + str(totalDataFiles) + "|" + str(totalDeleteFiles) + "\n")


output = open("icebergLog.csv", "w")
output.write("experiment|step|added data files|deleted data files|added records|deleted records|" + 
             "added files size|removed files size|changed partition count|total records|total data files" +
             "|total delete files\n")

current = os.path.join(experiment, file)
if os.path.isfile(current) :
    data = open(current, "r")
    processFile(data, output, experiment)
output.close()


