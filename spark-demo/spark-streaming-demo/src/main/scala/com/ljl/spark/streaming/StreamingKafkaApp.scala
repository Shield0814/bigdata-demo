package com.ljl.spark.streaming

import java.io.File
import java.util.function.Consumer
import java.util.{HashMap => JHashMap}

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

object StreamingKafkaApp {
    val RUNNING_FILE = new File("D:\\data\\_RUNNING")

    def main(args: Array[String]): Unit = {

        val topics = Array("test")
        val checkPointDir = "./checkpoint"
        //1. 创建或恢复StreamingContext
        val ssc = createContext(topics)

        //2. 启动Streaming应用
        ssc.start()

        //3. 等待中止
        ssc.awaitTermination()

    }


    def createContext(topics: Array[String]): StreamingContext = {

        //1. 初始化streamingContext
        val conf = new SparkConf()
            .setAppName("StreamingKafkaApp")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.backpressure.initialRate", "100")
            .set("spark.streaming.kafka.maxRatePerPartition", "61440") //records/second
            .setMaster("local[*]")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        //kafka相关参数
        val kafkaParams = new JHashMap[String, Object]()
        kafkaParams.put("bootstrap.servers", "bigdata116:9092,bigdata117:9092,bigdata118:9092")
        kafkaParams.put("key.deserializer", classOf[StringDeserializer])
        kafkaParams.put("value.deserializer", classOf[StringDeserializer])
        kafkaParams.put("group.id", "streaming_kafka_app_group")
        kafkaParams.put("auto.offset.reset", "latest")
        kafkaParams.put("enable.auto.commit", (false: java.lang.Boolean))


        //2. 从offset存储系统中获取上次消费到的kafka消息offset
        val consumer = new KafkaConsumer[String, String](kafkaParams)
        consumer.subscribe(topics.toList)
        val pInfos = consumer.partitionsFor(topics(0))
        val fromOffsets = Map[TopicPartition, Long]()
        pInfos.forEach(new Consumer[PartitionInfo] {
            override def accept(pinfo: PartitionInfo): Unit = {
                val tp = new TopicPartition(pinfo.topic(), pinfo.partition())
                val offsetMeta = consumer.committed(tp)
                if (offsetMeta != null) {
                    fromOffsets += (tp -> offsetMeta.offset())
                }
            }
        })
        println("上次消费到：" + fromOffsets)
        consumer.close()

        //3. 获取到Kafka的DStream
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (fromOffsets.size == 0) {
            //如果第一次消费，则直接创建
            inputDStream = KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
        } else {
            //否则从上次消费的offset开始消费
            inputDStream = KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
        }

        //4. 计算有状态的wordCount
        var offsetRanges = Array[OffsetRange]()
        var canCommitOffsets: CanCommitOffsets = null

        inputDStream.foreachRDD(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            canCommitOffsets = inputDStream.asInstanceOf[CanCommitOffsets]
        })

        inputDStream.transform(rdd => {
            rdd.map(record => record.value)
                .flatMap(_.split("\\W"))
                .map((_, 1))
        }).reduceByKeyAndWindow(reduceFunc = (v1, v2) => v1 + v2, windowDuration = Seconds(10), slideDuration = Seconds(5))
            .foreachRDD(rdd => {
                rdd.foreach(println)
                canCommitOffsets.commitAsync(offsetRanges)
            })

        //
        //        inputDStream.foreachRDD(rdd => {
        //            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //            println(s"rdd ${rdd.id} 中对应的kafka消息的offset信息 ${offsetRanges.mkString(",")}")
        //
        //            rdd.map(record => record.value)
        //                .flatMap(_.split("\\W"))
        //                .map((_, 1))
        //                .reduceByKey(_ + _)
        //                .foreachPartition(iter => {
        //                    iter.foreach(println)
        //                })
        //            inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //        })
        ssc
    }


}
