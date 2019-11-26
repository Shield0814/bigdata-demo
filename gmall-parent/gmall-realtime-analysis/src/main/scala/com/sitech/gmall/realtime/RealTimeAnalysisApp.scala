package com.sitech.gmall.realtime

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import com.alibaba.fastjson.JSON
import com.sitech.gmall.common.GmallConstants
import com.sitech.gmall.config.GmallConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 启动日志
  *
  * @param area  区域
  * @param uid   用户id
  * @param os    操作系统
  * @param ch    下载渠道
  * @param appid 应用id
  * @param mid   设备编号
  * @param vs    应用版本号
  * @param ts    时间
  */
case class StartUpEvent(area: String,
                        uid: Int,
                        os: String,
                        ch: String,
                        appid: String,
                        mid: String,
                        vs: String,
                        ts: Long)

class TodayMidAccumlator extends AccumulatorV2[StartUpEvent, mutable.Set[String]] {

    var accValue = mutable.Set[String]()

    override def isZero: Boolean = accValue.isEmpty

    override def copy(): AccumulatorV2[StartUpEvent, mutable.Set[String]] = {
        val newAcc = new TodayMidAccumlator
        newAcc.accValue = mutable.Set[String]()
        newAcc.accValue.addAll(accValue)
        newAcc
    }

    override def reset(): Unit = accValue.clear()

    override def add(v: StartUpEvent): Unit = accValue.add(v.mid)

    override def merge(other: AccumulatorV2[StartUpEvent, mutable.Set[String]]): Unit = {
        val o = other.asInstanceOf[TodayMidAccumlator]
        accValue.addAll(o.accValue)
    }

    override def value: mutable.Set[String] = accValue
}


object TodayMidStatistics {

    @volatile private var instance: TodayMidAccumlator = _

    def getInstance(sc: SparkContext) = {

        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = new TodayMidAccumlator
                    sc.register(instance, "TodayMidAccumlator")
                }
            }
        }
        instance
    }


}

object RealTimeAnalysisApp {

    val RUNNING_FLAG = new File("d:\\data\\REAL_TIME_RUNING")
    private val logger: Logger = LoggerFactory.getLogger(RealTimeAnalysisApp.getClass)

    def main(args: Array[String]): Unit = {

        //1. 创建StreamContext
        val conf = new SparkConf()
            .setAppName("RealTimeAnalysisApp")
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.backpressure.initialRate", "10240")
            .set("spark.streaming.kafka.maxRatePerPartition", "102400")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")
            .setMaster("local[*]")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))

        //有状态计算设置checkpoint目录
        val checkPointDir = "./checkPoint"
        ssc.checkpoint(checkPointDir)

        //2. 从kafka获得数据
        //kafka配置参数
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> GmallConfigs.KAKFA_BOOTSTRAP_SEVERS,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> "gmall-realtime",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
            ConsumerConfig.RECEIVE_BUFFER_CONFIG -> (65536: java.lang.Integer),
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> (500: java.lang.Integer),
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1024: java.lang.Integer)
        )
        //kafka topic:数据来源
        val topics = Array(GmallConstants.KAFKA_TOPIC_STARTUP)
        val startDStream = receiveDataFromKafka(ssc, topics, kafkaParams)


        //3. 实时分析

        //需求一: 当日用户首次登录（日活）分时趋势图，昨日对比
        val value = computeTimeShareActiveMid(startDStream, ssc)

        value.print()
        //优雅的停止
        //        stopGracefully(ssc)

        //4. 启动并等待sparkstreaming应用停止
        ssc.start()
        ssc.awaitTermination()
    }


    /**
      * 需求一: 当日用户首次登录（日活）分时趋势图，昨日对比
      * 需求分析：
      * 统计各个小时当日首次登录的用户数量
      * 已统计过的用户不做统计，即需要做去重处理
      *
      * @param startDStream
      */
    def computeTimeShareActiveMid(startDStream: DStream[StartUpEvent], ssc: StreamingContext) = {

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH")


        //从启动事件流中，过滤出今天没有登录过的mid，（mid,首次登录时间戳）
        val targetMidTsDStream = startDStream.mapPartitions(iter => {
            //redis客户端
            val jedis = new Jedis(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
            //过滤出今天未登录过的用户
            val unLoginMids = iter
                .filter(sue => !jedis.sismember(GmallConstants.REDIS_TODAY_MID_KEY, sue.mid))
                .map(sue => (sue.mid, sue.ts))
            jedis.close()
            unLoginMids
        }).reduceByKey(_.min(_))

        //计算每小时的用户登录次数
        targetMidTsDStream.map {
            case (mid, ts) =>
                (sdf.format(new Date(ts)), 1)
        }.updateStateByKey[Int]((cnts, state) => {
            state match {
                case Some(newState) =>
                    Some(newState + cnts.sum)
                case None =>
                    Some(cnts.sum)
            }
        })

    }


    /**
      * 从kafka读取数据
      *
      * @param topics      kafka topic
      * @param kafkaParams kafka 配置参数
      * @return
      */
    def receiveDataFromKafka(ssc: StreamingContext,
                             topics: Seq[String],
                             kafkaParams: Map[String, Object]): DStream[StartUpEvent] = {

        //1. 获取该消费者组已提交的offsets,保存到
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
        logger.error(s"上次各分区消费到: $fromOffsets")
        consumer.close()

        //2. 根据fromOffsets从kafka读取数据
        var startDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (fromOffsets.isEmpty) {
            //如果第一次消费，通过订阅的策略消费读取数据
            startDStream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets))
        } else {
            //如果是失败重启，则根据分配策略从上次消费到的offset开始消费
            startDStream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))
        }

        var offsetRanges: Array[OffsetRange] = null
        var canCommitOffsets: CanCommitOffsets = null
        startDStream.foreachRDD(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            canCommitOffsets = rdd.asInstanceOf[CanCommitOffsets]
            rdd
        })
        startDStream.map(record => {
            val jo = JSON.parseObject(record.value())
            //"area": "sichuan",
            //"uid": "100",
            //"os": "ios",
            //"ch": "appstore",
            //"appid": "gmall2019",
            //"mid": "mid_434",
            //"type": "startup",
            //"vs": "1.1.3",
            //"ts": 1574679074656
            StartUpEvent(jo.getString("area"),
                jo.getIntValue("uid"),
                jo.getString("os"),
                jo.getString("ch"),
                jo.getString("appid"),
                jo.getString("mid"),
                jo.getString("vs"),
                jo.getLongValue("ts"))
        })

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
