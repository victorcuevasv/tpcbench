package org.bsc.dcc.vcv;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     * 
     * [0] server hostname
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        // replace "hive" here with the name of the user the queries should run as
        //Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
        Connection con = DriverManager.getConnection("jdbc:hive2://" + args[0] + 
        		":10000/default", "hive", "");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create external table " + tableName + 
        		" (r_reason_sk int, r_reason_id char(16)," + 
        		" r_reason_desc char(100) ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'" + 
        		" STORED AS TEXTFILE" +  
        		" LOCATION '/temporal/1GB/reason'");
        
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2) + 
            		"\t" + res.getString(3));
        }

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2)
            	+ "\t" + res.getString(3));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}


