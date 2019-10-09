package com.ljl.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HbaseUtilsTest {
    private Admin hBaseAdmin;
    private Connection connection;

    /**
     * 初始化hbase客户端
     */
    @Before
    public void init() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata116,bigdata117,bigdata118");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            hBaseAdmin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭hbase客户端
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
        }
    }

    @Test
    public void testDeleteColumnByRowkey() {
        boolean res = HbaseUtils.deleteColumnByRowkey(connection, "student",
                "100041", "info", "gender");
        System.out.println(res);

    }

    @Test
    public void testDeleteMultiRow() {
        HbaseUtils.deleteMultiRow(connection, "student", new String[]{"100013", "100039"});
    }

    @Test
    public void testGetOneRowData() {
        List<Cell> cells = HbaseUtils.getOneRowData(connection, "student", "100013");
        iterCells(cells);
    }

    @Test
    public void testScanWithFilter() {
//        FilterList fiters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        FilterList fiters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("name1"));
        ColumnPrefixFilter cpf = new ColumnPrefixFilter(Bytes.toBytes("name"));

        PrefixFilter pf = new PrefixFilter(Bytes.toBytes("1000"));

        fiters.addFilter(scvf);
        fiters.addFilter(cpf);
        fiters.addFilter(pf);

        List<Cell> cells = HbaseUtils.scanWithFilter(connection,
                "student", new String[]{"info"},
                fiters, 50);
        iterCells(cells);
    }

    @Test
    public void testScanWithNoFilter() {
        List<Cell> cells = HbaseUtils.scanWithFilter(connection, "student", new String[]{"info"}, null, 50);
        iterCells(cells);
    }

    private void iterCells(List<Cell> cells) {
        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            byte[] rowkey = CellUtil.cloneRow(cell);
            System.out.println(Bytes.toString(rowkey) + "," + Bytes.toString(family) + ":"
                    + Bytes.toString(qualifier) + "," + Bytes.toString(value));
        }
    }

    @Test
    public void testPutData() {
        List<Put> puts = new ArrayList<>();

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            Put put = new Put(Bytes.toBytes("1000" + random.nextInt(100)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("name" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes("" + random.nextInt(1)));
            puts.add(put);
        }
        HbaseUtils.putData(connection, "student", puts);
    }

    @Test
    public void testCreateTable() {
        HbaseUtils.createTable(connection, "student", new String[]{"info", "grade"}, new String[]{}, 1, 3);
    }

    @Test
    public void testDeleteTable() {
        HbaseUtils.deleteTable(connection, "student");
    }


    @Test
    public void testGetTableDesc() {
        HbaseUtils.getTableDesc(connection, "student");
    }

    /**
     * 测试创建namespace
     */
    @Test
    public void testCreateNamespace() throws IOException {
        NamespaceDescriptor gmall = hBaseAdmin.getNamespaceDescriptor("gmall");
        if (gmall != null) {
            System.out.println("gmall命名空间已存在");
        } else {
            NamespaceDescriptor ns = NamespaceDescriptor.create("gmall")
                    .addConfiguration("desc", "电商项目相关")
                    .build();
            hBaseAdmin.createNamespace(ns);
        }


    }

    /**
     * 测试获取namespace相关api
     *
     * @throws IOException
     */
    @Test
    public void testNameSpaceApi() throws IOException {
        //获取所有namespace
        System.out.println("=============获取所有namespace================");
        NamespaceDescriptor[] namespaces = hBaseAdmin.listNamespaceDescriptors();
        for (int i = 0; i < namespaces.length; i++) {
            System.out.println(namespaces[i].getName());
        }
        //获取某个namespace
        System.out.println("=============获取某个namespace，检查namespace是否存在 ================");
        NamespaceDescriptor hbase = hBaseAdmin.getNamespaceDescriptor("gmall");
        if (hbase != null) {
            System.out.println(hbase.getName());
            hbase.getConfiguration().forEach((key, value) -> {
                System.out.println(key + "->" + value);
            });
        } else {
            System.out.println("hbase namespace不存在");
        }


    }
}
