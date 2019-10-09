package com.sitech.weibo.service;

import com.sitech.weibo.common.Constants;
import com.sitech.weibo.common.exception.NotSupportException;
import com.sitech.weibo.common.rowkey.Generator;
import com.sitech.weibo.dao.WeiboDao;
import com.sitech.weibo.entity.WeiboEntity;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class WeiboService {

    private WeiboDao weiboDao = new WeiboDao();

    private Logger logger = LoggerFactory.getLogger(WeiboService.class);

    //rowkey生成器
    private Generator generator;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public WeiboService(Generator generator) {
        this.generator = generator;
    }

    public Generator getGenerator() {
        return generator;
    }

    public void setGenerator(Generator generator) {
        this.generator = generator;
    }

    /**
     * 初始化微博项目命名空间和表
     *
     * @throws IOException
     */
    public void initTable() throws IOException {
        //1.初始化命名空间
        weiboDao.createNamespace(Constants.NAMESPACE_NAME);
        //2.初始化微博表
        //rowkey：userId|timestamp
        weiboDao.createTable(Constants.WEIBO_TABLE_NAME,
                new String[]{Constants.WEIBO_FAMILYS_NAME},
                0, null, true);

        //3.初始化用户关系表
        //rowkey: 0|fansId|starId; 其中0表示关注关系，即fansId关注了starId
        //rowkey: 1|startId|fansId; 其中1表示被关注关系，即startId被fansId关注
        weiboDao.createTable(Constants.RELATIONSHIP_TABLE_NAME,
                new String[]{Constants.RELATIONSHIP_FAMILYS_NAME},
                0, null, true);

        //4.初始化用户微博内容接收邮件表
        //rowkey: userId|starId
        weiboDao.createTable(Constants.INBOX_TABLE_NAME,
                new String[]{Constants.INBOX_FAMILYS_NAME},
                Constants.INBOX_VERSIONS, null, true);

        logger.info("初始化微博项目环境成功");
    }

    /**
     * 发布微博
     *
     * @param content 微博内容
     * @param userId  发表人
     */
    public void publish(String content, String userId) throws NotSupportException, IOException {
        //1. 像微博表添加微博信息
        long currentTimeMillis = System.currentTimeMillis();
        byte[] rowKey = generator.getRowKey(userId, Long.toString(currentTimeMillis));
        Put weibo = new Put(rowKey);
        //微博内容
        weibo.addColumn(Bytes.toBytes(Constants.WEIBO_FAMILYS_NAME),
                Bytes.toBytes("content"),
                Bytes.toBytes(content));
        //发布时间
        weibo.addColumn(Bytes.toBytes(Constants.WEIBO_FAMILYS_NAME),
                Bytes.toBytes("publishTime"),
                Bytes.toBytes(sdf.format(new Date(currentTimeMillis))));
        weiboDao.putData(Constants.WEIBO_TABLE_NAME, weibo);

        //2. 获得关注该用户的粉丝
        List<Cell> fans = listFans(userId);

        //3. 向粉丝收件箱表中插入该条微博rowkey
        if (!fans.isEmpty()) {
            //把该条微博插入粉丝收件箱表中
            List<Put> weiboToFans = fans.parallelStream()
                    .map(cell -> {
                        String fansId = Bytes.toString(CellUtil.cloneValue(cell));
                        try {
                            //收件箱表rowkey: fansId|userId
                            byte[] inboxRowkey = generator.getRowKey(fansId, userId);

                            Put put = new Put(inboxRowkey);
                            put.addColumn(Bytes.toBytes(Constants.INBOX_FAMILYS_NAME),
                                    Bytes.toBytes("starId"), Bytes.toBytes(userId));
                            put.addColumn(Bytes.toBytes(Constants.INBOX_FAMILYS_NAME),
                                    Bytes.toBytes("weiboId"), rowKey);

                            return put;
                        } catch (NotSupportException e) {
                            logger.error("rowkey生成器不支持这种调用方法,将导致不能插入该记录");
                            return null;
                        }
                    }).filter(x -> x != null).collect(Collectors.toList());
            weiboDao.putData(Constants.INBOX_TABLE_NAME, weiboToFans);
        }
        logger.info("用户" + userId + "发布了一条微博");

    }


    /**
     * 关注某人
     *
     * @param fansId 关注人
     * @param starId 被关注的人
     */
    public void followSomeone(String fansId, String starId) throws NotSupportException, IOException {
        //向关系表中插入两条记录，分别表示:
        // 1. fansId关注了starId,
        // 2. starId被fansId关注
        byte[] followRowKey = generator.getRowKey("0", fansId, starId);
        byte[] followedRowKey = generator.getRowKey("1", starId, fansId);

        List<Put> relations = new ArrayList<>();
        //关注关系数据
        Put follow = new Put(followRowKey);
        follow.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("userId"), Bytes.toBytes(fansId));
        follow.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("targetUserId"), Bytes.toBytes(starId));
        follow.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("relationDesc"), Bytes.toBytes("follow"));
        relations.add(follow);

        //被关注关系数据
        Put followed = new Put(followedRowKey);
        followed.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("userId"), Bytes.toBytes(starId));
        followed.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("targetUserId"), Bytes.toBytes(fansId));
        followed.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes("relationDesc"), Bytes.toBytes("followed"));
        relations.add(followed);

        //插入用户关系表
        weiboDao.putData(Constants.RELATIONSHIP_TABLE_NAME, relations);

        //获取被关注用户的最近3条微博，插入到用户收件箱表中
        byte[] weiboRowPrefix = generator.getRowKey(starId);
        List<String> weiboIds = weiboDao.scanByPrefix(Constants.WEIBO_TABLE_NAME, weiboRowPrefix,
                Constants.WEIBO_FAMILYS_NAME, null)
                .stream().map(result -> Bytes.toString(result.getRow()))
                .collect(Collectors.toList());
        //如果关注的人没有发布过微博，则直接返回关注成功
        if (weiboIds.isEmpty())
            return;
        else {
            Collections.sort(weiboIds, (o1, o2) -> o2.compareTo(o1));
            int endIndex = weiboIds.size() >= Constants.INBOX_VERSIONS ? Constants.INBOX_VERSIONS : weiboIds.size();
            List<Put> recentWeibos = weiboIds.subList(0, endIndex)
                    .stream().map(weiboId -> {
                        try {
                            byte[] inboxRowkey = generator.getRowKey(fansId, starId);
                            Put put = new Put(inboxRowkey);
                            put.addColumn(Bytes.toBytes(Constants.INBOX_FAMILYS_NAME),
                                    Bytes.toBytes("starId"), Bytes.toBytes(starId));
                            put.addColumn(Bytes.toBytes(Constants.INBOX_FAMILYS_NAME),
                                    Bytes.toBytes("weiboId"), Bytes.toBytes(weiboId));
                            return put;
                        } catch (NotSupportException e) {
                            logger.error("rowkey生成器不支持这种调用方法,将导致不能插入该记录");
                            return null;
                        }
                    }).filter(put -> put != null).collect(Collectors.toList());
            weiboDao.putData(Constants.INBOX_TABLE_NAME, recentWeibos);
        }

        logger.info("用户" + fansId + "关注了用户" + starId);


    }

    /**
     * 取关某人
     *
     * @param fansId
     * @param starId
     */
    public void unFollowSomeone(String fansId, String starId) throws NotSupportException, IOException {
        //更新用户关系表中的userId关注startId和starId被userId关注的数据，为其添加删除标记deleted,并标识为1
        byte[] followRowKey = generator.getRowKey("0", fansId, starId);
        byte[] followedRowKey = generator.getRowKey("1", starId, fansId);
        List<Put> relationsToBeDelete = new ArrayList<>();
        //删除关注记录
        Put follow = new Put(followRowKey);
        follow.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes(Constants.DELETE_FLAG), Bytes.toBytes("1"));
        relationsToBeDelete.add(follow);

        //删除被关注记录
        Put followed = new Put(followedRowKey);
        followed.addColumn(Bytes.toBytes(Constants.RELATIONSHIP_FAMILYS_NAME),
                Bytes.toBytes(Constants.DELETE_FLAG), Bytes.toBytes("1"));
        relationsToBeDelete.add(followed);

        //更新用户关系表
        weiboDao.putData(Constants.RELATIONSHIP_TABLE_NAME, relationsToBeDelete);

        //删除用户收件箱表中该用户关注的微博博主的微博记录，即不再看该微博博主的微博信息
        byte[] rowKey = generator.getRowKey(fansId, starId);
        weiboDao.deleteDataByRowKey(Constants.INBOX_TABLE_NAME, rowKey);

        logger.info("用户" + fansId + "去关了用户" + starId);

    }

    /**
     * 获取某个用户的粉丝
     *
     * @param starId
     * @return
     * @throws NotSupportException
     * @throws IOException
     */
    private List<Cell> listFans(String starId) throws NotSupportException, IOException {
        byte[] rowPrefix = generator.getRowKey("1", starId);
        List<Cell> fans = weiboDao.scanByPrefix(Constants.RELATIONSHIP_TABLE_NAME, rowPrefix,
                Constants.RELATIONSHIP_FAMILYS_NAME, "targetUserId", Constants.DELETE_FLAG)
                .stream().filter(result -> {
                    List<Cell> cells = result.listCells();
                    for (Cell cell : cells) {
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        if (Constants.DELETE_FLAG.equals(qualifier) && "1".equals(value)) {
                            return false;
                        }
                    }
                    return true;
                }).map(result -> result.listCells().get(0)).collect(Collectors.toList());
        return fans;
    }

    /**
     * 获取某个用户关注的所有人最近发布的3条微博
     *
     * @param fansId 粉丝标识
     * @return
     */
    public List<WeiboEntity> getStarsWeibo(String fansId) throws NotSupportException, IOException {
        //1. 获取该粉丝关注的所有人最近发布的3条微博的weiboId
        byte[] rowPrefix = generator.getRowKey(fansId);
        //1.1 获取该粉丝收件箱中关注用户对应的所有rowkey
        List<byte[]> inboxIds = weiboDao.scanByPrefix(Constants.INBOX_TABLE_NAME, rowPrefix,
                Constants.INBOX_FAMILYS_NAME, "weiboId")
                .stream().map(result -> result.getRow()).collect(Collectors.toList());

        //1.2 获取每个inboxId对应的所有版本的 weiboId
        List<byte[]> weiboIds = new ArrayList<>();
        for (int i = 0; i < inboxIds.size(); i++) {
            weiboDao.getCellByRowkeyAndVersion(Constants.INBOX_TABLE_NAME, inboxIds.get(i),
                    Constants.INBOX_FAMILYS_NAME, "weiboId", Constants.INBOX_VERSIONS)
                    .stream().map(cell -> CellUtil.cloneValue(cell))
                    .forEach(weiboId -> weiboIds.add(weiboId));
        }

        //2. 获取每条微博的微博内容
        List<WeiboEntity> weibos = weiboDao.getDataByRowkeys(Constants.WEIBO_TABLE_NAME, weiboIds)
                .stream().map(result -> {
                    List<Cell> cells = result.listCells();
                    String weiboId = Bytes.toString(result.getRow());
                    String weiboContent = null;
                    String publishTime = null;
                    for (Cell cell : cells) {
                        if ("content".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                            weiboContent = Bytes.toString(CellUtil.cloneValue(cell));
                        }
                        if ("publishTime".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                            publishTime = Bytes.toString(CellUtil.cloneValue(cell));
                        }
                    }
                    return new WeiboEntity(weiboId, weiboContent, publishTime);
                }).collect(Collectors.toList());
        logger.info("获得微博信息" + weibos.size() + "条");
        return weibos;
    }

    /**
     * 获取某个用户最近发布的微博
     *
     * @param userId
     * @param limit  返回微博记录数
     * @return
     */
    public List<WeiboEntity> getWeiboByUserId(String userId, int limit) throws NotSupportException, IOException {
        byte[] rowPrefix = generator.getRowKey(userId);
        List<WeiboEntity> webos = weiboDao.scanByPrefixWithLimit(Constants.WEIBO_TABLE_NAME, rowPrefix, limit,
                Constants.WEIBO_FAMILYS_NAME, "content", "publishTime")
                .stream().map(result -> {
                    String weiboId = Bytes.toString(result.getRow());
                    String weiboContent = null;
                    String publishTime = null;
                    for (Cell cell : result.listCells()) {
                        if ("content".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                            weiboContent = Bytes.toString(CellUtil.cloneValue(cell));
                        }
                        if ("publishTime".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                            publishTime = Bytes.toString(CellUtil.cloneValue(cell));
                        }
                    }
                    return new WeiboEntity(weiboId, weiboContent, publishTime);
                }).collect(Collectors.toList());
        return webos;
    }
}
