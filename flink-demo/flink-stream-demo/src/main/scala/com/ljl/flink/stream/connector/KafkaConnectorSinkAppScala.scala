package com.ljl.flink.stream.connector

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaConnectorSinkAppScala {

    def main(args: Array[String]): Unit = {

        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        val data = senv.socketTextStream("bigdata116", 9999)
            .map(System.currentTimeMillis() + "_" + _)

        kafkaConnectorAsSink(data, senv)
    }

    /**
      * kafka作为sink，把数据写到kafka 中的一个topic中
      *
      * @param data 数据流
      * @param senv 执行环境
      */
    def kafkaConnectorAsSink(data: DataStream[String],
                             senv: StreamExecutionEnvironment): Unit = {

        val brokerList = "bigdata116:9092,bigdata117:9092,bigdata118:9092"
        val topicId = "flink-connector-kafka-sink"
        val serializationSchema = new SimpleStringSchema()
        val kafkaSink = new FlinkKafkaProducer011(brokerList, topicId, serializationSchema)

        data.addSink(kafkaSink)
        senv.execute("kafkaConnectorAsSink")
    }


}
