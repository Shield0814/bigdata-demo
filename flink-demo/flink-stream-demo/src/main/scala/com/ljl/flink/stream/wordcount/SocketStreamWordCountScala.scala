package com.ljl.flink.stream.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object SocketStreamWordCountScala {

    def main(args: Array[String]): Unit = {

        val tool = ParameterTool.fromArgs(args)

        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        var lines: DataStream[String] = null
        if (tool.has("host") && tool.has("port")) {
            lines = senv.socketTextStream(tool.get("host"), tool.getInt("port"), '\n', 2)
        } else {
            throw new IllegalArgumentException("主机，端口号不能为空")
        }

        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(5))
            .sum(1)
            .print().setParallelism(1)

        senv.execute("scala scoket stream wordcount")

    }

}
