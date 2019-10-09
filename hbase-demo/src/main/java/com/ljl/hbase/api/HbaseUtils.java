package com.ljl.hbase.api;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HbaseUtils {

    private static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);


    public static boolean deleteColumn() {

        return false;
    }


    /**
     * 删除表中某行某列的数据
     *
     * @param conn
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     */
    public static boolean deleteColumnByRowkey(Connection conn, String tableName,
                                               String rowKey, String family, String qualifier) {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            tbl.delete(del);
        } catch (IOException e) {
            logger.error("删除失败", e);
            return false;
        }
        return true;
    }

    /**
     * 删除多行数据
     *
     * @param conn      hbase 数据库连接
     * @param tableName 表名称
     * @param rowKeys   rowkey列表
     * @return
     */
    public static boolean deleteMultiRow(Connection conn, String tableName, String[] rowKeys) {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for (int i = 0; i < rowKeys.length; i++) {
                Delete del = new Delete(Bytes.toBytes(rowKeys[i]));
                deletes.add(del);
            }
            tbl.delete(deletes);
        } catch (IOException e) {
            logger.error("删除失败", e);
            return false;
        }
        return true;
    }

    /**
     * 根据条件扫描表数据，如果filter条件为空，扫描所有数据
     *
     * @param conn      hbase数据库连接
     * @param tableName 表名称
     * @param families  列族
     * @param filters   过滤条件
     * @param limit     返回rowkey的个数
     * @return
     */
    public static List<Cell> scanWithFilter(Connection conn, String tableName, String[] families, FilterList filters, int limit) {

        Table tbl = null;
        try {
            if (limit > 0) {
                //获得table实例
                tbl = conn.getTable(TableName.valueOf(tableName));
                //构造扫描条件
                Scan scan = new Scan();
                for (int i = 0; i < families.length; i++) {
                    scan.addFamily(Bytes.toBytes(families[i]));
                }
                if (filters != null) {
                    scan.setFilter(filters);
                }
                ResultScanner scanner = tbl.getScanner(scan);

                //遍历结果
                int counter = 0;
                List<Cell> result = new ArrayList<>();
                Iterator<Result> iter = scanner.iterator();
                while (counter <= limit && iter.hasNext()) {
                    result.addAll(iter.next().listCells());
                    counter++;
                }
                logger.info("返回数据行数为:" + --counter);
                return result;
            } else {
                logger.error("limit 期望值为大于0,传入值为:" + limit);
                return null;
            }
        } catch (IOException e) {
            logger.error("获取数据时发生错误", e);
            return null;
        }
    }


    /**
     * 获取一行数据
     *
     * @param conn      hbase数据库连接
     * @param tableName 表名称
     * @param get       发送的get请求
     * @return
     */
    public static List<Cell> getOneRowData(Connection conn, String tableName, Get get) {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            Result rs = tbl.get(get);
            return rs.listCells();
        } catch (IOException e) {
            logger.error("获取数据异常");
            return null;
        }
    }


    /**
     * 根据 rowkey 获取一行所有数据
     *
     * @param conn      hbase 数据库连接
     * @param tableName tableName
     * @param rowKey
     * @return
     */
    public static List<Cell> getOneRowData(Connection conn, String tableName, String rowKey) {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result rs = tbl.get(get);
            return rs.listCells();
        } catch (IOException e) {
            logger.error("获取数据异常:" + rowKey);
            return null;
        }
    }

    /**
     * 插入数据
     *
     * @param conn  hbase 数据库连接
     * @param table 表名称
     * @param puts  要插入的数据
     * @return
     */
    public static boolean putData(Connection conn, String table, List<Put> puts) {
        Table tbl = null;
        try {
            tbl = conn.getTable(TableName.valueOf(table));
            tbl.put(puts);
        } catch (IOException e) {
            logger.error("插入数据失败", e);
            return false;
        }
        return true;
    }

    /**
     * 获取hbase表信息
     *
     * @param conn
     * @param tableName
     */
    public static void getTableDesc(Connection conn, String tableName) {
        Admin hbaseAdmin = null;
        try {
            hbaseAdmin = conn.getAdmin();
            HTableDescriptor tblDesc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            long memStoreFlushSize = tblDesc.getMemStoreFlushSize();
            String splitPolicy = tblDesc.getRegionSplitPolicyClassName();
            System.out.println("table properties -> memStoreFlushSize:" + memStoreFlushSize);
            System.out.println("table properties -> splitPolicy:" + splitPolicy);

            HColumnDescriptor[] columnFamilies = tblDesc.getColumnFamilies();
            for (int i = 0; i < columnFamilies.length; i++) {
                System.out.println("=====================================");
                int blocksize = columnFamilies[i].getBlocksize();
                String cfName = columnFamilies[i].getNameAsString();
                String compressName = columnFamilies[i].getCompression().getName();
                int maxVersions = columnFamilies[i].getMaxVersions();
                int minVersions = columnFamilies[i].getMinVersions();
                System.out.println("columfamily properties -> cfName:" + cfName);
                System.out.println("columfamily properties -> blocksize:" + blocksize);
                System.out.println("columfamily properties -> compressName:" + compressName);
                System.out.println("columfamily properties -> maxVersions:" + maxVersions);
                System.out.println("columfamily properties -> minVersions:" + minVersions);
            }

        } catch (IOException e) {
            logger.error("获取表描述信息异常", e);
        }
    }

    /**
     * 删除表
     *
     * @param conn      hbase 数据库连接
     * @param tableName 表名
     * @return 若表删除成功或不存在返回删除成功
     */
    public static boolean deleteTable(Connection conn, String tableName) {
        Admin hBaseAdmin = null;
        try {
            hBaseAdmin = conn.getAdmin();
            if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
                //删除表需要先 disable，再delete
                hBaseAdmin.disableTable(TableName.valueOf(tableName));
                hBaseAdmin.deleteTable(TableName.valueOf(tableName));
            } else {
                logger.warn("表不存在:" + tableName);
                return true;
            }
        } catch (IOException e) {
            logger.error("删除表时发生异常", e);
            return false;
        }
        return true;
    }


    /**
     * 创建hbase表
     *
     * @param conn       hbase 数据库连接
     * @param tableName  表名
     * @param splitKeys  预分区键
     * @param minVersion 最小列族版本
     * @param maxVersion 最大列族版本
     * @return
     */
    public static boolean createTable(Connection conn, String tableName, String[] familys,
                                      String[] splitKeys, int minVersion, int maxVersion) {
        Admin hBaseAdmin = null;
        try {
            hBaseAdmin = conn.getAdmin();
            if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
                logger.info("表已存在：" + tableName);
                return true;
            } else {
                //1. 检查列族数量合法性
                if (familys.length > 3) {
                    logger.error("列族数量超过3，将严重影响性能");
                    return false;
                } else if (familys == null || familys.length == 0) {
                    logger.error("必须指定列族");
                }
                //2. 创建表
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                for (int i = 0; i < familys.length; i++) {
                    HColumnDescriptor f = new HColumnDescriptor(familys[i]);
                    f.setMaxVersions(maxVersion);
                    f.setMinVersions(minVersion);
                    f.setCompressionType(Compression.Algorithm.SNAPPY);
                    desc.addFamily(f);

                }

                hBaseAdmin.createTable(desc, Bytes.toByteArrays(splitKeys));
            }
        } catch (IOException e) {
            logger.error("创建表时发生异常", e);
            return false;
        }
        return true;
    }


    /**
     * 创建 namespace
     *
     * @param conn hbase 数据库连接
     * @param ns   namespace
     * @param conf namespace 配置描述信息
     * @return 是否创建成功，如果已存在也返回true
     */
    public static boolean createNamespace(Connection conn, String ns, Map<String, String> conf) {
        Admin hBaseAdmin = null;

        try {
            hBaseAdmin = conn.getAdmin();
            NamespaceDescriptor nsDesc = hBaseAdmin.getNamespaceDescriptor(ns);
        } catch (NamespaceNotFoundException e) {
            logger.info("namespace " + ns + " 不存在, 准备创建");
            try {
                hBaseAdmin = conn.getAdmin();
                NamespaceDescriptor nsBuild = NamespaceDescriptor.create(ns)
                        .addConfiguration(conf)
                        .build();
                hBaseAdmin.createNamespace(nsBuild);
            } catch (IOException ioe) {
                logger.error("创建namespace时发生异常", ioe);
            }
            return true;
        } catch (IOException ioe) {
            logger.error("创建namespace时发生异常", ioe);
        }
        return false;

    }
}
