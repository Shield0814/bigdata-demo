package com.ljl.flume.source;

import com.ljl.flume.util.JdbcUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 需求：自定义FLume Source实现实时拉取数据库表中的被修改的数据记录
 */
public class JdbcTableSource extends AbstractSource implements Configurable, PollableSource {

    private long BACKOFF_SLEEP_INCREMENT = 1000;
    private long MAX_BACKOFF_SLEEP_INTERVAL = 10000;

    private static final Logger logger = LoggerFactory.getLogger(JdbcTableSource.class);
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //jdbc连接相关信息
    private static final String JDBC_URL = "JDBC_URL";
    private String jdbcUrl;
    private static final String JDBC_DRIVER = "JDBC_DRIVER";
    private String driver;
    private static final String JDBC_USER = "JDBC_USER";
    private String user;
    private static final String JDBC_PASSWORD = "JDBC_PASSWORD";
    private String password;
    private Connection conn;

    //读取表数据sql
    private static final String DATA_EXTRACT_SQL = "EXTRACT_SQL";
    private String sql;

    //数据开始时间
    private static final String START_TIME = "START_TIME";
    private volatile Date startTime;


    //上批次抽取数据的结束时间
    private volatile Date lastExtractTime;

    /**
     * agent启动时调用
     * 初始化外部客户端的连接
     */
    @Override
    public synchronized void start() {
        try {
            conn = JdbcUtils.getConnection(driver, jdbcUrl, user, password);
            lastExtractTime = new Date();
        } catch (ClassNotFoundException e) {
            logger.error("jdbc驱动类找不到", e);
        } catch (SQLException e) {
            logger.error("获取数据库连接错误", e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        super.start();
    }

    /**
     * agent停止时调用
     * 关闭数据库连接
     */
    @Override
    public synchronized void stop() {
        super.stop();
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 拉取数据
     * flume内部将循环调用该方法，所以不用在该方法中考虑循环问题，只需处理数据
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        List<Event> events;
        try {

            events = extractData();

            if (!events.isEmpty()) {

                getChannelProcessor().processEventBatch(events);


            }
            TimeUnit.SECONDS.sleep(1);
            return Status.READY;
        } catch (Exception e) {
            logger.error("抽取数据时发生错误", e);
        }
        return Status.BACKOFF;
    }

    /**
     * 根据 EXTRACT_DATA_SQL 抽取数据库中的被修改的数据
     *
     * @return
     */
    private List<Event> extractData() throws SQLException, ParseException {
        lastExtractTime = new Date();
        if (logger.isInfoEnabled())
            logger.info("prepare extract data: startTime=" + startTime + ",lastExtractTime" + lastExtractTime);

        List<String> data = JdbcUtils.getQueryResult(conn, sql, startTime, lastExtractTime);

        if (!data.isEmpty()) {
            startTime = lastExtractTime;
            if (logger.isInfoEnabled())
                logger.info("next extract data: startTime=" + startTime);

            return data.stream().map(line -> {
                SimpleEvent event = new SimpleEvent();
                event.setBody(line.getBytes());
                return event;
            }).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }

    }

    /**
     * 拉取数据失败后等待重试的间隔
     *
     * @return
     */
    @Override
    public long getBackOffSleepIncrement() {
        return BACKOFF_SLEEP_INCREMENT;
    }

    /**
     * 拉取数据失败后最大等待的时间
     *
     * @return
     */
    @Override
    public long getMaxBackOffSleepInterval() {
        return MAX_BACKOFF_SLEEP_INTERVAL;
    }

    @Override
    public void configure(Context context) {
        driver = context.getString(JDBC_DRIVER);
        jdbcUrl = context.getString(JDBC_URL);
        user = context.getString(JDBC_USER);
        password = context.getString(JDBC_PASSWORD);
        sql = context.getString(DATA_EXTRACT_SQL);
        try {
            startTime = sdf.parse(context.getString(START_TIME));
        } catch (ParseException e) {
            logger.warn("数据开始时间解析异常，使用当前时间作为数据开始时间", e);
            startTime = new Date();
        }
    }


}