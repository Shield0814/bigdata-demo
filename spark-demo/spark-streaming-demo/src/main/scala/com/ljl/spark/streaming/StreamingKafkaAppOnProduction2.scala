package com.ljl.spark.streaming

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object StreamingKafkaAppOnProduction2 {

    private val logger: Logger = LoggerFactory.getLogger(StreamingKafkaAppOnProduction2.getClass)

    var RUNNING_FLAG: File = new File("./RUNNING")

    def main(args: Array[String]): Unit = {


        //创建 StreamingContext
        val conf = new SparkConf()
            .setAppName("RealTimeAnalysisApp")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.backpressure.initialRate", "10240")
            .set("spark.streaming.kafka.maxRatePerPartition", "102400")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))

        // kafka 配置参数
        val kafkaParams = Map[String, String](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata116:9092,bigdata117:9092,bigdata118:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.GROUP_ID_CONFIG -> "gmall-realtime",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.RECEIVE_BUFFER_CONFIG -> "65536",
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "500",
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> "1024"
        )

        val topics = Set("test")

        //获取数据
        val kc = new KafkaCluster(kafkaParams)
        val inputDStream = receiveDataFromKafka(ssc, kc, kafkaParams, topics)

        //处理数据
        processBussiness(inputDStream, kc, kafkaParams("group.id"))

        ssc.start()
        ssc.awaitTermination()

    }

    /**
      * 处理数据
      *
      * @param inputDStream
      * @param kc
      * @return
      */
    def processBussiness(inputDStream: InputDStream[(String, String)], kc: KafkaCluster, groupId: String) = {
        var offsets: Map[TopicAndPartition, Long] = null
        inputDStream.transform(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            //转换offset成map
            offsets = (mutable.Map[TopicAndPartition, Long]() /: offsetRanges) ((offsets, offsetRange) =>
                offsets += offsetRange.topicAndPartition() -> offsetRange.untilOffset).toMap
            logger.error(s"本批次offset为: ${offsets.mkString(",")}")
            rdd.map(_._2).flatMap(_.split("\\W")).map((_, 1))
        }).reduceByKey(_ + _)
            .foreachRDD(rdd => {
                rdd.foreach(println)
                //提交offset
                commitOffsets(kc, groupId, offsets)
            })
    }


    /**
      * 从 kakfa 对接数据
      *
      * @param ssc
      * @param kafkaParams
      * @param topics
      * @return
      */
    def receiveDataFromKafka(ssc: StreamingContext,
                             kc: KafkaCluster,
                             kafkaParams: Map[String, String],
                             topics: Set[String]): InputDStream[(String, String)] = {
        //1. 获得已提交的offset
        val fromOffsets = getFromOffsets(kc, kafkaParams("group.id"), topics)

        //2. 创建 DirectKafkaInputDStream
        var inputDStream: InputDStream[(String, String)] = null
        //如果存在已提交的offset，那么根据已提交的offset进行创建，否则根据kafka配置相关参数创建
        val messageHandler = (message: MessageAndMetadata[String, String]) => (message.key(), message.message())
        inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaParams, fromOffsets, messageHandler)

        inputDStream
    }

    /**
      * 提交offsets
      *
      * @param kc
      * @param fromOffsets
      */
    def commitOffsets(kc: KafkaCluster, groupId: String, fromOffsets: Map[TopicAndPartition, Long]) = {
        kc.setConsumerOffsets(groupId, fromOffsets)
        logger.error(s"提交offset: ${fromOffsets.mkString(",")}")
    }

    /**
      * 获得该kafka消费者组订阅topic各个分区的offset
      *
      * @param kc
      * @return
      */
    def getFromOffsets(kc: KafkaCluster,
                       groupId: String,
                       topics: Set[String]): Map[TopicAndPartition, Long] = {
        //获得 topic 分区信息
        val tps = kc.getPartitions(topics).right.get
        //获得 offset
        val fromOffsetsEither = kc.getConsumerOffsets(groupId, tps)
        //如果存在已提交的offset，则返回，否则返回null
        if (fromOffsetsEither.isRight) {
            fromOffsetsEither.right.get
        } else {
            tps.map((_, 0L)).toMap
        }
    }

    /**
      * 实现sparkstreaming程序优雅的停止
      */
    def stopGracefully(ssc: StreamingContext): Unit = {
        val pool = Executors.newSingleThreadScheduledExecutor()

        //每隔5s检查sparkstreaming运行时标签是否存在，
        // 如不存在，优雅停止sparkstreaming
        pool.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = {
                if (!RUNNING_FLAG.exists()) {
                    ssc.stop(true, true)
                    pool.shutdown()
                    System.exit(0)
                }
            }
        }, 10L, 5, TimeUnit.SECONDS)

    }
}
