package com.ljl.flink.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class CustomRichSinkFunction extends RichSinkFunction[String] {

    var connection: Connection = _
    var pstm: PreparedStatement = _


    /**
      * sink写数据
      *
      * @param value   输入记录
      * @param context 上下文
      */
    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
        val fields = value.split("\\s")
        pstm.setString(1, fields(0))
        pstm.setString(2, fields(1))
        pstm.setFloat(3, fields(2).toFloat)
        pstm.setFloat(4, fields(3).toFloat)
        pstm.executeUpdate()
        connection.commit()
    }


    /**
      * 打开sink的时候初始化
      *
      * @param parameters 配置参数
      */
    override def open(parameters: Configuration): Unit = {
        val driverClass = parameters.getString("jdbc_driver_class", "com.mysql.jdbc.Driver")
        val url = parameters.getString("jdbc_url", "jdbc:mysql://localhost:3306/test?allowMultiQueries=true")
        val user = parameters.getString("jdbc_user", "root")
        val password = parameters.getString("jdbc_password", "root")
        connection = getConnection(driverClass, url, user, password)
        connection.setAutoCommit(false)
        pstm = connection.prepareStatement("insert into bonus(ename,job,sal,comm) values(?,?,?,?)")
    }

    /**
      * 获得数据库连接
      *
      * @param driverClass 驱动类
      * @param url         数据库连接url
      * @param user        用户名
      * @param password    密码
      * @return 数据库连接
      */
    def getConnection(driverClass: String, url: String, user: String, password: String): Connection = {
        Class.forName(driverClass)
        DriverManager.getConnection(url, user, password)
    }

    /**
      * 关闭sink
      */
    override def close(): Unit = {
        if (connection != null && !connection.isClosed) {
            connection.close()
        }
    }
}
