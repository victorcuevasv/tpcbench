package com.qbeast.opensourcedev

class QueryRecord (val query: Int, val run: Int, val startTime: Long, val endTime: Long, val successful: Boolean, 
                   val resultsSize: Long, val tuples: Int) {  
}