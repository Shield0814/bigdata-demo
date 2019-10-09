package com.sitech.weibo.dao;

import com.sitech.weibo.common.SysConfig;
import com.sun.istack.internal.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class WeiboDao {

    private Logger logger = LoggerFactory.getLogger(WeiboDao.class);

    private static Connection conn;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", SysConfig.sysConfig.getProperty("zk.server"));
            conf.set("hbase.zookeeper.property.clientPort", SysConfig.sysConfig.getProperty("zk.clientPort"));
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowkey删除数据
     *
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public void deleteDataByRowKey(String tableName, byte[] rowkey) throws IOException {
        Table tbl = conn.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(rowkey);
        tbl.delete(del);
        logger.info(tableName + "删除了1条数据");
    }

    /**
     * 获取rowkey 不同版本的列数据
     *
     * @param tableName 表名称
     * @param rowKey
     * @param family    列族
     * @param column    列
     * @param version
     * @return
     * @throws IOException
     */
    public List<Cell> getCellByRowkeyAndVersion(String tableName, byte[] rowKey, String family, String column, int version) throws IOException {
        Table tbl = null;
        tbl = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey);
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        get.setMaxVersions(version);
        Result result = tbl.get(get);
        return result.listCells();
    }


    /**
     * 向表中插入一组数据
     *
     * @param tableName 表名称
     * @param puts      要插入的数据
     * @return
     * @throws IOException
     */
    public boolean putData(String tableName, List<Put> puts) throws IOException {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            for (Put put : puts) {
                tbl.put(put);
            }
        } finally {
            tbl.close();
        }
        logger.info("向表" + tableName + "插入" + puts.size() + "条记录");
        return true;
    }


    /**
     * 向表中插入一条数据
     *
     * @param tableName 表
     * @param put       要插入的数据
     * @return
     * @throws IOException
     */
    public boolean putData(String tableName, Put put) throws IOException {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            tbl.put(put);
            logger.info("向表" + tableName + "插入1条记录");
        } finally {
            tbl.close();
        }
        return true;
    }


    /**
     * 创建表
     *
     * @param tableName 表名称
     * @param families  列族列表
     * @param version   最大版本数
     * @param splitKeys 切分key
     * @param compress  是否压缩列族，若为true，采用snappy压缩，集群必须支持才可以
     * @return
     */
    public boolean createTable(@NotNull String tableName, @NotNull String[] families, int version,
                               String[] splitKeys, boolean compress) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                //创建表描述
                HTableDescriptor tblDesc = new HTableDescriptor(TableName.valueOf(tableName));
                if (families.length == 0 || families.length > 3) {
                    throw new IllegalArgumentIOException("列族不能为空并且不能超过3个");
                } else {
                    //添加列族描述
                    for (int i = 0; i < families.length; i++) {
                        HColumnDescriptor f = new HColumnDescriptor(families[i]);
                        if (version > 0) {
                            f.setMaxVersions(version);
                        }
                        if (compress) {
                            f.setCompressionType(Compression.Algorithm.SNAPPY);
                        }
                        tblDesc.addFamily(f);
                    }
                }
                //若分区key不为空，则创建预分区表，否则创建普通表
                if (splitKeys != null && splitKeys.length != 0) {
                    admin.createTable(tblDesc, Bytes.toByteArrays(splitKeys));
                    logger.info("成功创建预分区表: " + tableName + ",  分区键为:" + Arrays.asList(splitKeys));
                } else {
                    admin.createTable(tblDesc);
                    logger.info("成功创建表: " + tableName);
                }
                return true;
            }
        } finally {
            admin.close();
        }
        return false;
    }

    /**
     * 创建namespace
     *
     * @param ns namespace
     * @return
     * @throws IOException
     */
    public boolean createNamespace(String ns) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            admin.getNamespaceDescriptor(ns);
        } catch (NamespaceNotFoundException nnfe) {
            NamespaceDescriptor nsDesc = NamespaceDescriptor.create(ns).build();
            admin.createNamespace(nsDesc);
            return true;
        } finally {
            admin.close();
        }
        return false;
    }

    /**
     * /**
     * 根据rowkey前缀查找数据
     *
     * @param tableName 表名称
     * @param rowPrefix row前缀
     * @param limit     最大返回结果数
     * @param family    列族
     * @param columns   列
     * @return
     * @throws IOException
     */
    public List<Result> scanByPrefixWithLimit(@NotNull String tableName, @NotNull byte[] rowPrefix, @NotNull int limit,
                                              @NotNull String family, String... columns) throws IOException {
        List<Result> result = scanByPrefix(tableName, rowPrefix, family, columns);
        return result.subList(0, limit);
    }


    /**
     * 根据rowkey前缀查找数据
     *
     * @param tableName 表名称
     * @param rowPrefix row前缀
     * @param family    列族
     * @param columns   列
     * @return
     * @throws IOException
     */
    public List<Result> scanByPrefix(@NotNull String tableName, @NotNull byte[] rowPrefix,
                                     @NotNull String family, String... columns) throws IOException {
        List<Result> result = new ArrayList<>();
        Table tbl = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (columns != null && columns.length > 0) {
            for (int i = 0; i < columns.length; i++) {
                scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(columns[i]));
            }
        }
        //开启客户端缓存块，mr不建议开启
        scan.setCacheBlocks(true);
        //设置客户端scanner每次拉取的行记录数，
        // （根据数据块估算得出：每次拉取128M数据，每行数据1kb，所以为128*1024 = 121072）
        scan.setCaching(131072);
        scan.setRowPrefixFilter(rowPrefix);
        ResultScanner scanner = tbl.getScanner(scan);
        Iterator<Result> iter = scanner.iterator();
        while (iter.hasNext()) {
            Result next = iter.next();
            result.add(next);
        }
        tbl.close();
        return result;
    }

    /**
     * 根据rowkey列表获取数据
     *
     * @param rowkeys rowkey列表
     */
    public List<Result> getDataByRowkeys(@NotNull String tableName, @NotNull List<byte[]> rowkeys) throws IOException {
        Table tbl = null;
        List<Result> result = new ArrayList<>();
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            List<Get> gets = rowkeys.stream().map(rk -> new Get(rk)).collect(Collectors.toList());
            Result[] results = tbl.get(gets);
            for (int i = 0; i < results.length; i++) {
                result.add(results[i]);
            }
        } finally {
            tbl.close();
        }
        return result;
    }


}
