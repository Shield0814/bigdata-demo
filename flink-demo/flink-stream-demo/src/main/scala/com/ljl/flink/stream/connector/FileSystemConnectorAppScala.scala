package com.ljl.flink.stream.connector


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * 测试文件系统connector
  * 需求：把从socket中读到的数据加上时间戳实时写入到hdfs中
  *
  */
object FileSystemConnectorAppScala {

    def main(args: Array[String]): Unit = {

        //1. 获取流处理运行环境
        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        //2. 从socket读取数据
        val lines = senv.socketTextStream("bigdata116", 9999)
            .map(System.currentTimeMillis() + "_" + _)
            .setParallelism(2)

        //3. 初始化FileSystem Connector
        var basePath = "hdfs://bigdata116:8020/tmp/flink-conector/fs-connector"
        //        var basePath = "file:///d:/data/flink-conector/fs-connector"
        val sink = new BucketingSink[String](basePath)
        //设置文件名称的格式
        sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
        //设置writer
        sink.setWriter(new StringWriter[String]())
        //设置批次，即：每多少条生成一个批次写到文件
        sink.setBatchSize(1024 * 1024)
        //设置滚动间隔，每隔多长时间滚动文件，这里设置60s
        sink.setBatchRolloverInterval(60 * 1000)


        //4. 输入数据流绑定sink
        lines.addSink(sink).setParallelism(2)

        //5. 启动
        senv.execute("FileSystemConnectorAppScala")

    }


}
