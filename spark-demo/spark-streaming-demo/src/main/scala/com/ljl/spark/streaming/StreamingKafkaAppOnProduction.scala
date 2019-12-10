package com.ljl.spark.streaming

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.function.Consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

object StreamingKafkaAppOnProduction {

    private val logger: Logger = LoggerFactory.getLogger(StreamingKafkaAppOnProduction.getClass)

    var RUNNING_FLAG: File = new File("./RUNNING")

    def main(args: Array[String]): Unit = {

        //从命令行获取运行时标签文件,checkpoint 目录
        //        val (runningFlag, checkpointDir) = parseParams(args)
        val (runningFlag, checkpointDir) = ("./RUNNING", "D:\\data\\checkpoint")
        RUNNING_FLAG = new File(runningFlag)

        //streaming 配置相关
        val conf = new SparkConf()
            .setAppName("RealTimeAnalysisApp")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.backpressure.initialRate", "10240")
            .set("spark.streaming.kafka.maxRatePerPartition", "102400")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .setMaster("local[4]")

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
        val topics = Array("test")


        //创建streamingcontext
        val ssc = createStreamingContext(conf, checkpointDir, Seconds(5))

        //获取数据流
        val inputDStream = receiveDataFromKafka(ssc, kafkaParams, topics)

        //处理数据
        processExactlyOnce(inputDStream)

        //启动应用
        ssc.start()
        ssc.awaitTermination()

    }

    /**
      * 精确一次统计wordcount
      *
      * @param inputDStream
      */
    def processExactlyOnce(inputDStream: InputDStream[ConsumerRecord[String, String]]): Unit = {

        var offsetRanges: Array[OffsetRange] = null
        var canCommitOffsets: CanCommitOffsets = null

        inputDStream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            logger.info(s"获取到的offset为: ${offsetRanges.mkString(",")}")
            canCommitOffsets = inputDStream.asInstanceOf[CanCommitOffsets]
            rdd.map(_.value())
        }).flatMap(_.split("\\W"))
            .map((_, 1))
            .updateStateByKey((counts: Seq[Int], state: Option[Int]) => {
                state match {
                    case Some(count) =>
                        Some(count + counts.sum)
                    case None =>
                        Some(counts.sum)
                }
            }).foreachRDD(rdd => {
            println("=" * 40)
            rdd.foreach(println)
            canCommitOffsets.commitAsync(offsetRanges)
            logger.error(s"提交offset: ${offsetRanges.mkString(",")}")
        })
    }


    /**
      * 从kafka读取数据
      * 为了保证至少一次消费，我们手动提交kafka消费者组的offset
      *
      * @param kafkaParams
      * @param topics
      */
    def receiveDataFromKafka(ssc: StreamingContext,
                             kafkaParams: Map[String, Object],
                             topics: Seq[String]) = {
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
        if (fromOffsets.isEmpty) {
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
        inputDStream
    }


    /**
      * 创建streamingContext
      *
      * @param conf
      * @param checkPointDir checkpoint dir
      * @param batchInterval 批次时间
      * @return
      */
    def createStreamingContext(conf: SparkConf,
                               checkPointDir: String,
                               batchInterval: Duration) = {
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, batchInterval)
        ssc.checkpoint(checkPointDir)
        ssc
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

    /**
      * 从命令行参数中解析运行时标签文件和checkpoint地址
      *
      * @param args
      * @return
      */
    def parseParams(args: Array[String]) = {
        var checkpointDir = ""
        var runningFlag = ""
        if (args.length != 0) {
            //解析运行时标签文件
            if (args.contains("--running-flag")) {
                val flagIdx = args.indexOf("--running-flag") + 1
                if (flagIdx < args.length && !args(flagIdx).startsWith("--")) {
                    runningFlag = args(flagIdx)
                }
            }
            //解析checkpoint地址
            if (args.contains("--checkpointdir")) {
                val chkIdx = args.indexOf("--checkpointdir") + 1
                if (chkIdx < args.length && !args(chkIdx).startsWith("--")) {
                    checkpointDir = args(chkIdx)
                }
            } equals {
                logger.warn("streaming 应用checkpoint目录未设置")
            }

        } else {
            logger.info("streaming 应用运行时标签文件未设置, 默认将设置为:./RUNNING")
        }
        (runningFlag, checkpointDir)
    }
}
