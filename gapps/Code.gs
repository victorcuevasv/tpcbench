function doRun() {
  var resultsSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("RESULTS");
  var summarySheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("SUMMARY");
  clearResultsSheet(resultsSheet);
  clearSummarySheet(summarySheet);
  processMainFolder(resultsSheet, summarySheet);
}


function clearResultsSheet(resultsSheet) {
  var columns = ['TEST', 'STREAM', 'QUERY', 'SUCCESSFUL', 'DURATION', 'RESULTS_SIZE', 'SYSTEM', 
                 'STARTDATE_EPOCH', 'STOPDATE_EPOCH', 'DURATION_MS', 'STARTDATE', 'STOPDATE'];
  resultsSheet.clear();
  resultsSheet.appendRow(columns);
  var headerRange = resultsSheet.getRange(1, 1, 1, columns.length);
  headerRange.setFontWeight('bold');
}

function clearSummarySheet(summarySheet) {
  var columns = ['SYSTEM', 'TEST', 'TOTAL_TIME_SEC', 'QUERY_ARITH_MEAN_SEC', 'QUERY_GEOM_MEAN_SEC'];
  summarySheet.clear();
  summarySheet.appendRow(columns);
  var headerRange = summarySheet.getRange(1, 1, 1, columns.length);
  headerRange.setFontWeight('bold');
}

function processMainFolder(resultsSheet, summarySheet) {
  var mainFolderId = '1x-XsmBlMN0LjwjPE9H9Szfz8mmjew1Kd'; 
  var mainFolder = DriveApp.getFolderById(mainFolderId);
  var systemFolders = mainFolder.getFolders();
  while (systemFolders.hasNext()) {
    var systemFolder = systemFolders.next();
    var systemFolderName = systemFolder.getName();
    var experimentFolders = systemFolder.getFolders();
    while(experimentFolders.hasNext()) {
      var experimentFolder = experimentFolders.next();
      var experimentFolderName = experimentFolder.getName();
      processExperiment(systemFolderName, experimentFolder, experimentFolderName, resultsSheet, summarySheet);
    }
  }
}

function processExperiment(systemFolderName, experimentFolder, experimentFolderName, resultsSheet, summarySheet) {
  var files = experimentFolder.getFilesByName('analytics.log');
  //The TEST and STREAM columns are missing for the analyze, load, and power tests.
  var missingCols = [experimentFolderName];
  if( experimentFolderName.localeCompare('tput') != 0 ) {
    missingCols.push(NaN)
  }
  var rows = [];
  while(files.hasNext()) {
    var file = files.next();
    var fileContents = file.getBlob().getDataAsString().trim();
    var lines = fileContents.split('\n');
    //Skip the header line
    for (var index = 1; index < lines.length; ++index) {
      var values = lines[index].split('|');
      //Set the value of the element at index 4 or 5, which contains the system.
      if( experimentFolderName.localeCompare('tput') != 0 )
        values[4] = systemFolderName;
      else
        values[5] = systemFolderName;
      var row = missingCols.slice(0).concat(values);
      rows.push(row);
    }
  }
  resultsSheet.getRange(resultsSheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  summarizeExperiment(systemFolderName, experimentFolderName, rows, summarySheet);
}

function summarizeExperiment(systemFolderName, experimentFolderName, rows, summarySheet) {
  var values = [systemFolderName, experimentFolderName];
  var durations = rows.map(function(value, index) { return value[4]; });
  var sumDuration = durations.reduce(function(acc, val) { return +acc + +val; }, 0);
  var prodDuration = durations.reduce(function(acc, val) { return +acc * +val; }, 1);
  var arithMeanDuration = sumDuration / rows.length;
  var geomMeanDuration = Math.pow(prodDuration, 1.0 / rows.length);
  if( experimentFolderName.localeCompare('tput') != 0 ) {
    values.push(sumDuration);
  }
  else {
    var minStartDateEpoch = Math.min.apply(null, rows.map(function(value, index) { return +value[7]; }));
    var maxStopDateEpoch = Math.max.apply(null, rows.map(function(value, index) { return +value[8]; }));
    var totalTime = maxStopDateEpoch - minStartDateEpoch;
    values.push(totalTime);
  }
  values.push(arithMeanDuration);
  values.push(geomMeanDuration);
  summarySheet.appendRow(values);
}

