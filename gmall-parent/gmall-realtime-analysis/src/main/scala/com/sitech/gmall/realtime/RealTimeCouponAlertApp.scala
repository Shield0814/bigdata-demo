package com.sitech.gmall.realtime

import com.sitech.gmall.common.GmallConstants
import com.sitech.gmall.realtime.util.{KafkaUtils, StreamingContextUtils}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds

object RealTimeCouponAlertApp {

    def main(args: Array[String]): Unit = {
        //kafka配置参数
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata116:9092,bigdata117:9092,bigdata118:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> "gmall-realtime",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
            ConsumerConfig.RECEIVE_BUFFER_CONFIG -> (65536: java.lang.Integer),
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> (500: java.lang.Integer),
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1024: java.lang.Integer)
        )

        //kafka topic
        val topics = Array(GmallConstants.KAFKA_TOPIC_EVENT)

        val ssc = StreamingContextUtils.createStreamingContext(
            "RealTimeOrderInfoApp",
            "local[*]",
            null,
            Seconds(5))

        val inputDStream = KafkaUtils.getDirectStream(ssc, kafkaParams, topics)


        StreamingContextUtils.startStreamingApplication(ssc, null)

        StreamingContextUtils.stopGracefully(ssc, null)
    }
}
