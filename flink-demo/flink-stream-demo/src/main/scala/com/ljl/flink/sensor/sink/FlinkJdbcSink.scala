package com.ljl.flink.sensor.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 基于幂等性的Jdbc 的 sink
  *
  */
class FlinkJdbcSink extends RichSinkFunction[SensorReading] {


    private val logger: Logger = LoggerFactory.getLogger(FlinkJdbcSink.getClass)

    //jdbc连接
    var connection: Connection = _

    //插入PreparedStatement
    var insertPstmt: PreparedStatement = _

    //更新PreparedSatement
    var updatePstmt: PreparedStatement = _


    override def open(params: Configuration): Unit = {
        //检查需要的配置是否存在,并获得配置信息
        val (url, driverClass, username, password, insertSql, updateSql) = FlinkJdbcSink.checkConfig(params)
        Class.forName(driverClass)
        connection = DriverManager.getConnection(url, username, password)
        // insert into alert_sensor(sensor_id,alert_temperature,alert_ts) values(?,?,?)
        insertPstmt = connection.prepareStatement(insertSql)
        // update alert_sensor set alert_temperature = ?, alert_ts = ? where sensor_id = ?
        updatePstmt = connection.prepareStatement(updateSql)

        if (logger.isInfoEnabled()) {
            logger.info("FlinkJdbcSink初始化完成:\n" +
                s"数据库连接信息: ${connection},\n" +
                s"插入sql语句: ${insertSql}\n" +
                s"修改sql语句: ${updateSql}")
        }
    }


    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        updatePstmt.setDouble(1, value.temperature)
        updatePstmt.setLong(2, value.timestamp)
        updatePstmt.setString(3, value.id)
        if (updatePstmt.getUpdateCount == 0) {
            insertPstmt.setString(1, value.id)
            insertPstmt.setDouble(2, value.temperature)
            insertPstmt.setLong(3, value.timestamp)
        }
    }

    override def close(): Unit = {
        if (insertPstmt != null) {
            insertPstmt.close()
        }
        if (updatePstmt != null) {
            updatePstmt.close()
        }
        if (connection != null && !connection.isClosed) {
            connection.close()
        }
    }

}

object FlinkJdbcSink {

    class Builder {

        var jdbcUrl: String = _
        var driverClass: String = _
        var userName: String = _
        var password: String = _
        var insertSql: String = _
        var updateSql: String = _

    }


    val FLINK_SINK_JDBC_URL = "flink.sink.jdbc.url"
    val FLINK_SINK_JDBC_DRIVER_CLASS = "flink.sink.jdbc.driverClass"
    val FLINK_SINK_JDBC_USERNAME = "flink.sink.jdbc.username"
    val FLINK_SINK_JDBC_PASSWORD = "flink.sink.jdbc.username"
    val FLINK_SINK_JDBC_INSERT_SQL = "flink.sink.jdbc.sql.insert"
    val FLINK_SINK_JDBC_UPDATE_SQL = "flink.sink.jdbc.sql.update"

    def checkConfig(params: Configuration) = {
        var url, driverClass, username, password, insertSql, updateSql = ""
        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_URL)) {
            throw new IllegalArgumentException(s"未配置jdbc连接url,使用${FlinkJdbcSink.FLINK_SINK_JDBC_URL}参数配置")
        } else {
            url = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_URL, "")
        }

        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_DRIVER_CLASS)) {
            throw new IllegalArgumentException(s"未配置jdbc驱动,使用${FlinkJdbcSink.FLINK_SINK_JDBC_DRIVER_CLASS}参数配置")
        } else {
            driverClass = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_DRIVER_CLASS, null)
        }

        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_USERNAME)) {
            throw new IllegalArgumentException(s"未配置数据库用户名,使用${FlinkJdbcSink.FLINK_SINK_JDBC_USERNAME}参数配置")
        } else {
            username = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_USERNAME, null)
        }

        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_PASSWORD)) {
            throw new IllegalArgumentException(s"未配置数据库密码,使用${FlinkJdbcSink.FLINK_SINK_JDBC_PASSWORD}参数配置")
        } else {
            password = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_PASSWORD, null)
        }
        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_INSERT_SQL)) {
            throw new IllegalArgumentException(s"未配置新增数据SQL,使用${FlinkJdbcSink.FLINK_SINK_JDBC_INSERT_SQL}参数配置")
        } else {
            insertSql = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_INSERT_SQL, null)
        }
        if (!params.containsKey(FlinkJdbcSink.FLINK_SINK_JDBC_UPDATE_SQL)) {
            throw new IllegalArgumentException(s"未配置修改数据SQL,使用${FlinkJdbcSink.FLINK_SINK_JDBC_UPDATE_SQL}参数配置")
        } else {
            updateSql = params.getString(FlinkJdbcSink.FLINK_SINK_JDBC_UPDATE_SQL, null)
        }
        (url, driverClass, username, password, insertSql, updateSql)
    }
}