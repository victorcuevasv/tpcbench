import os
import sys
import json

def processFile(data, output, experiment, step, counter):
    Lines = data.readlines()
    for line in Lines:
        if ( line.startswith("{\"add\"") ) :
            processAddLine(line, output, experiment, step, counter)
        elif ( line.startswith("{\"remove\"") ) :
            processRemoveLine(line, output, experiment, step, counter)
        else :
            continue
    
def convertAddLineToJSON(line) :
    #Find the numRecords field and skip everything after that field value,
    #which is marked by the comma after the value.
    posNR = line.find('numRecords')
    posComma = line.find(',', posNR)
    lineCut = line[:posComma]
    #Convert the stats text into a json object in three steps.
    #First, remove the \ character.
    lineCut = lineCut.replace('\\', '')
    #Second, remove the last " before the last {.
    posLeftBrace = lineCut.rfind('{')
    lineCut = lineCut[:posLeftBrace-1] + lineCut[posLeftBrace:]
    #Third, add the 3 missing right braces at the end.
    lineCut = lineCut + "}}}" 
    return json.loads(lineCut)

def convertRemoveLineToJSON(line) :
    return json.loads(line)
    
def processAddLine(line, output, experiment, step, counter) :
    counter['added'] += 1
    addedJSON = convertAddLineToJSON(line)
    path = addedJSON["add"]["path"]
    records = addedJSON["add"]["stats"]["numRecords"]
    size = addedJSON["add"]["size"]
    partition = addedJSON["add"]["partitionValues"]["ss_sold_date_sk"]
    output.write(experiment + "|" + str(step) + "|" + "add" + "|" + path + "|" + str(records) 
                 + "|" + str(size) + "|" + str(partition) + "\n")
    
def processRemoveLine(line, output, experiment, step, counter) :
    counter['removed'] += 1
    removedJSON = convertRemoveLineToJSON(line)
    path = removedJSON["remove"]["path"]
    #The numRecords field is not present for remove
    records = 0
    size = removedJSON["remove"]["size"]
    partition = removedJSON["remove"]["partitionValues"]["ss_sold_date_sk"]
    output.write(experiment + "|" + str(step) + "|" + "remove" + "|" + path + "|" + str(records) 
                 + "|" + str(size) + "|" + str(partition) + "\n")


output = open("log.csv", "w")
output.write("experiment|step|operation|path|records|size|partition\n")
step = 0
counter = {'added':0, 'removed':0}
experiment = sys.argv[1]
for file in sorted(os.listdir(experiment)):
    current = os.path.join(experiment, file)
    if os.path.isfile(current) :
        data = open(current, "r")
        processFile(data, output, experiment, step, counter)
    step += 1
output.close()

print("added: " + str(counter['added']))
print("removed: " + str(counter['removed']))



