package com.ljl.presto;

import com.facebook.presto.jdbc.PrestoConnection;

import java.sql.*;

public class PrestoClient {

    public static void main(String[] args) throws SQLException {
//        String url = "jdbc:presto://example.net:8080/hive/sales";
//        Properties properties = new Properties();
//        properties.setProperty("user", "test");
//        properties.setProperty("password", "secret");
//        properties.setProperty("SSL", "true");
//        Connection connection = DriverManager.getConnection(url, properties);

        String url = "jdbc:presto://bigdata116:8080/hive/rwd_dev?user=sysadm&password=";
        PrestoConnection conn = (PrestoConnection) DriverManager.getConnection(url);

        System.out.println(conn);

        Statement stat = conn.createStatement();
        ResultSet resultSet = stat.executeQuery("select * from rwd_dev.emp");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + ",");
            }
            System.out.println();
        }
    }
}
