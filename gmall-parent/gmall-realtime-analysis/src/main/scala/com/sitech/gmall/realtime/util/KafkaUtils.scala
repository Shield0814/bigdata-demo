package com.sitech.gmall.realtime.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, LocationStrategies, OffsetRange, KafkaUtils => StreamKafkaUtils}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object KafkaUtils {

    private val logger: Logger = LoggerFactory.getLogger(KafkaUtils.getClass)


    /**
      * 通过kafka consumer提交offset
      *
      * @param offsetRanges
      * @param committer
      */
    def commitOffsets(offsetRanges: Array[OffsetRange], committer: KafkaConsumer[String, String]) = {

    }

    /**
      * 提交 offset
      *
      * @param offsetRanges
      * @param canCommitOffsets
      */
    def commitOffsets(offsetRanges: Array[OffsetRange], canCommitOffsets: CanCommitOffsets) = {
        canCommitOffsets.commitAsync(offsetRanges, new OffsetCommitCallback {
            override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
                if (exception != null) {
                    logger.error(s"提交${offsets.mkString(",")}时发生错误", exception)
                } else {
                    logger.info(s"成功提交${offsets.mkString(",")}")
                }
            }
        })
        logger.info(s"提交offset${offsetRanges.mkString(",")}")
    }

    /**
      * SparkStreaming获得到 kafka 的数据流
      *
      * @param ssc
      * @param kafkaParams kafka 配置参数
      * @param topics      要消费的topic
      */
    def getDirectStream(ssc: StreamingContext,
                        kafkaParams: Map[String, Object],
                        topics: Seq[String]) = {

        val fromOffsets = getFromOffsets(kafkaParams, topics)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (fromOffsets.isEmpty) {
            //如果第一次消费，通过订阅的策略消费读取数据
            inputDStream = StreamKafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets))
        } else {
            //如果是失败重启，则根据分配策略从上次消费到的offset开始消费
            inputDStream = StreamKafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
        }
        inputDStream
    }

    /**
      * 获得 consumergroup 已消费kafka数据的offset,
      * kafkaParams 中必须配置消费者组id：group.id
      *
      * @param kafkaParams
      * @param topics
      */
    def getFromOffsets(kafkaParams: Map[String, Object], topics: Seq[String]) = {
        //获取该消费者组已提交的offsets,保存到fromOffsets
        val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
        val consumer = new KafkaConsumer[String, String](kafkaParams)
        consumer.subscribe(topics)
        for (topic <- topics) {
            //获取topic的分区信息
            val pInfos = consumer.partitionsFor(topic)
            for (pinfo <- pInfos) {
                val tp = new TopicPartition(pinfo.topic(), pinfo.partition())
                //获取topic的分区已提交offset信息，保存到fromOffsets中
                val offsetMeta = consumer.committed(tp)
                if (offsetMeta != null) {
                    fromOffsets += (tp -> offsetMeta.offset())
                }
            }
        }
        logger.info(s"上次各分区消费到: $fromOffsets")
        consumer.close()
        fromOffsets.toMap
    }
}
