package com.ljl.flink.stream.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object KafkaConnectorSourceAppScala {

    def main(args: Array[String]): Unit = {

        val senv = StreamExecutionEnvironment.getExecutionEnvironment

        kafkaConnectorAsSource(senv)
    }

    /**
      * kafka作为source消费kafka数据，把数据从kafka读取出来消费
      *
      * @param senv 执行环境
      */
    def kafkaConnectorAsSource(senv: StreamExecutionEnvironment): Unit = {
        val brokerList = "bigdata116:9092,bigdata117:9092,bigdata118:9092"
        val zkConStr = "bigdata116:2181,bigdata117:2181,bigdata118:2181"
        val topicId = "flink-connector-kafka-sink"

        val props = new Properties()
        props.setProperty("bootstrap.servers", brokerList)
        props.setProperty("group.id", "flink-consumer-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")

        val kafkaSource = new FlinkKafkaConsumer011[String](topicId, new SimpleStringSchema(), props)
        senv.addSource(kafkaSource)
            .flatMap(_.split("\\W"))
            .map(w => {
                if (w.contains("_")) {
                    val strings = w.split("_")
                    (strings(1), 1)
                } else {
                    (w, 1)
                }

            })
            .keyBy(0)
            .sum(1)
            .print()

        senv.execute("KafkaConnectorSourceAppScala")
    }


}
