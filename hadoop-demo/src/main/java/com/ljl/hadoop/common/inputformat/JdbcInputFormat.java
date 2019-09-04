package com.ljl.hadoop.common.inputformat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcInputFormat extends InputFormat<Text, MapWritable> {

    private Logger logger = Logger.getLogger(JdbcInputFormat.class);
    //数据库连接相关属性
    private final static String MAPRED_INPUTFORMAT_JDBC_DRIVER = "mapreduce.inputformat.jdbc.driver";
    private final static String MAPRED_INPUTFORMAT_JDBC_USER = "mapreduce.inputformat.jdbc.user";
    private final static String MAPRED_INPUTFORMAT_JDBC_PASSWORD = "mapreduce.inputformat.jdbc.password";
    private final static String MAPRED_INPUTFORMAT_JDBC_URL = "mapreduce.inputformat.jdbc.url";

    //获取数据相关
    private final static String MAPRED_INPUTFORMAT_JDBC_TABLE = "mapreduce.inputformat.jdbc.table";
    private final static String MAPRED_INPUTFORMAT_JDBC_SQL = "mapreduce.inputformat.jdbc.sql";

    //数据切分相关
    private final static String MAPRED_INPUTFORMAT_JDBC_SPLITSIZE = "mapreduce.inputformat.jdbc.splitsize";


    /**
     * 数据库切片元数据
     */
    class DBInputSplit extends InputSplit implements Writable {
        //切片中数据开始和结束的id
        private int start;
        private int end;

        public DBInputSplit() {
        }

        @Override
        public long getLength() {
            return end - start;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }

        public DBInputSplit(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(start);
            out.write(end);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.start = in.readInt();
            this.end = in.readInt();
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        List<InputSplit> splits = new ArrayList<>();
        Configuration conf = context.getConfiguration();
        Connection conn = createConnection(conf);
        int splitSize = conf.getInt(MAPRED_INPUTFORMAT_JDBC_SPLITSIZE, 100000);
        String sql = conf.get(MAPRED_INPUTFORMAT_JDBC_SQL);
        String table = conf.get(MAPRED_INPUTFORMAT_JDBC_TABLE);
        if (!StringUtils.isEmpty(sql)) {
            String countSql = "select count(1) as total from (" + sql + ") t";
            int total = Integer.parseInt(query(conn, countSql).get(0).get(new Text("total")).toString());
            return createSplits(total, splitSize);
        } else {
            if (!StringUtils.isEmpty(table)) {
                String countSql = "select count(1) as from total from " + table + " t";
                int total = Integer.parseInt(query(conn, countSql).get(0).get(new Text("total")).toString());
                return createSplits(total, splitSize);
            }
        }
        return splits;
    }

    /**
     * 根据数据总行数和切片行数创建切片
     *
     * @param total     数据总行数
     * @param splitSize 数据切片大小
     * @return 切片信息
     */
    public List<InputSplit> createSplits(int total, int splitSize) {
        ArrayList<InputSplit> splits = new ArrayList<>();
        int remaining = total;
        int start = 0;
        //当剩余数据大于切片大小的1.1倍时，创建切片，以防止产生太小的切片
        while (((double) remaining / splitSize) > 1.1) {
            remaining -= splitSize;
            DBInputSplit split = new DBInputSplit(start, start + splitSize);
            splits.add(split);
            start = start + splitSize;
        }
        if (remaining != 0) {
            DBInputSplit split = new DBInputSplit(start, start + remaining);
            splits.add(split);
        }
        return splits;
    }

    /**
     * 执行sql查询
     *
     * @param sql sql语句
     * @return sql查询结果
     */
    public List<Map<Text, Text>> query(Connection conn, String sql) {
        List<Map<Text, Text>> result = new ArrayList<>();

        try {
            PreparedStatement stat = conn.prepareStatement(sql);
            ResultSet rs = stat.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();
            while (rs.next()) {
                Map<Text, Text> res = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String tmp = rs.getString(1);
                    Text txtRes = new Text(tmp);
                    res.put(new Text(rsMeta.getColumnLabel(i)), txtRes);
                }
                result.add(res);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取数据库连接
     *
     * @param conf job配置
     */
    public Connection createConnection(Configuration conf) {
        String driverClass = conf.get(MAPRED_INPUTFORMAT_JDBC_DRIVER, "com.mysql.jdbc.Driver");
        String url = conf.get(MAPRED_INPUTFORMAT_JDBC_URL, "jdbc:mysql://localhost:3306/test?allowMultiQueries=true");
        String user = conf.get(MAPRED_INPUTFORMAT_JDBC_USER, "root");
        String pwd = conf.get(MAPRED_INPUTFORMAT_JDBC_PASSWORD, "root");
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(url, user, pwd);
        } catch (Exception e) {
            logger.error("数据库驱动类：" + driverClass + "找不到,获取数据库连接失败", e);
            throw new RuntimeException("获取数据库连接异常");
        }

    }

    @Override
    public RecordReader<Text, MapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new JdbcRecordReader();
    }

    class JdbcRecordReader extends RecordReader<Text, MapWritable> {

        //数据库连接
        int count = 0;
        private Connection conn;
        private List<Map<Text, Text>> data;

        //key value
        private Text currentKey = new Text();
        private MapWritable currentValue = new MapWritable();


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            Configuration conf = context.getConfiguration();
            conn = createConnection(conf);
            DBInputSplit inputSplit = (DBInputSplit) split;
            String sql = conf.get(MAPRED_INPUTFORMAT_JDBC_SQL);
            String table = conf.get(MAPRED_INPUTFORMAT_JDBC_TABLE);
            if (!StringUtils.isEmpty(sql)) {
                sql = "set @rowNO=0;\n" +
                        "select * from (\n" +
                        "   select ( @rowNO := @rowNo + 1 ) AS rowno,t.* \n" +
                        "   from (" + sql + ") t \n" +
                        ") d\n" +
                        "where d.rowno >=" + inputSplit.getStart() + "\n" +
                        "and d.rowno < " + inputSplit.getEnd();
            } else {
                if (!StringUtils.isEmpty(table)) {
                    sql = "set @rowNO=0;\n" +
                            "select * from (\n" +
                            "   select ( @rowNO := @rowNo + 1 ) AS rowno,t.* \n" +
                            "   from (" + table + ") t \n" +
                            ") d\n" +
                            "where d.rowno >=" + inputSplit.getStart() + "\n" +
                            "and d.rowno < " + inputSplit.getEnd();
                }
            }
            data = query(conn, sql);
        }

        @Override
        public boolean nextKeyValue() {
            if (count <= data.size()) {
                Map<Text, Text> rowData = data.get(count);
                for (Map.Entry<Text, Text> row : rowData.entrySet()) {
                    currentValue.put(row.getKey(), row.getValue());
                }
                currentKey.set(String.valueOf(count++));
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() {
            return this.currentKey;
        }

        @Override
        public MapWritable getCurrentValue() {
            return this.currentValue;
        }

        @Override
        public float getProgress() {
            return ((float) count) / data.size();
        }

        @Override
        public void close() {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
