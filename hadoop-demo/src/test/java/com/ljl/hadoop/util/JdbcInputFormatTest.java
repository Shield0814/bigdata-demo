package com.ljl.hadoop.util;

import com.ljl.hadoop.common.inputformat.JdbcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JdbcInputFormatTest {

    private static JdbcInputFormat jdbcInputFormat = new JdbcInputFormat();
    private Connection conn;

    @Test
    public void testCreateSplits() {
        List<InputSplit> splits = jdbcInputFormat.createSplits(101, 10);
        System.out.println(splits);
    }

    @Test
    public void testQuery() {
        List<Map<Text, Text>> query = jdbcInputFormat.query(conn, "select count(1) from (select * from tb_test) t");
        System.out.println(query);

    }

    @Before
    public void testCreateConnection() {
        conn = jdbcInputFormat.createConnection(new Configuration());
        System.out.println(conn);
    }

    @After
    public void testClose() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
