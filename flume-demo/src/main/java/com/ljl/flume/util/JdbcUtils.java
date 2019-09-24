package com.ljl.flume.util;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JdbcUtils {

    /**
     * 获取数据库连接实例
     *
     * @param driver   驱动类
     * @param url      数据库连接
     * @param user     用户名
     * @param password 密码
     * @return 数据库连接实例
     */
    public static Connection getConnection(String driver, String url, String user, String password)
            throws ClassNotFoundException, SQLException,
            IllegalAccessException, InstantiationException {

        Class<Driver> driverClazz = (Class<Driver>) Class.forName(driver);
        DriverManager.registerDriver(driverClazz.newInstance());
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

    /**
     * 执行sql获得查询结果
     *
     * @param conn
     * @param sql
     * @param params
     * @return
     */
    public static List<String> getQueryResult(Connection conn, String sql, Date... params) throws SQLException, ParseException {
        List<String> data = new ArrayList<>();
        PreparedStatement pstm = conn.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            pstm.setTimestamp(i + 1, new Timestamp(params[i].getTime()));
        }
        ResultSet resultSet = pstm.executeQuery();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                sb.append(resultSet.getString(i)).append(",");
            }
            data.add(sb.substring(0, sb.length() - 1));
        }
        return data;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String url = "jdbc:mysql://127.0.0.1/test";
        String name = "com.mysql.jdbc.Driver";
        String user = "root";
        String password = "root";
        Connection conn = getConnection(name, url, user, password);
        String sql = "select * from emp where hiredate >= ? and hiredate < ?";

        Date date = new Date(1569307656139L);
        System.out.println(date);
        System.out.println(new Timestamp(1569307656139L));

        List<String> data = getQueryResult(conn, sql,
                new Timestamp(sdf.parse("1980-12-17").getTime()),
                new Timestamp(sdf.parse("1982-02-13").getTime()));

        data.forEach(System.out::println);
    }
}
