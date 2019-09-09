package com.ljl.flink.stream.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamSinkFunctionApp {

    def main(args: Array[String]): Unit = {

        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream = senv.socketTextStream("bigdata116", 9999)

        val parameters = new Configuration()
        parameters.setString("jdbc_driver_class", "com.mysql.jdbc.Driver")
        parameters.setString("jdbc_url", "jdbc:mysql://localhost:3306/test?allowMultiQueries=true")
        parameters.setString("jdbc_user", "root")
        parameters.setString("jdbc_password", "root")
        senv.getConfig.setGlobalJobParameters(parameters)

        dataStream.addSink(new CustomRichSinkFunction)

        senv.execute("StreamSinkFunctionApp")
    }

}
