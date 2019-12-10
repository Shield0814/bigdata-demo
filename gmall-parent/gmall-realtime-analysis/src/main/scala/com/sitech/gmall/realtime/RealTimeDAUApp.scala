package com.sitech.gmall.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.sitech.gmall.common.GmallConstants
import com.sitech.gmall.config.GmallConfigs
import com.sitech.gmall.realtime.acc.RealTimeDAUAccumulator
import com.sitech.gmall.realtime.util.{KafkaUtils, RedisUtils, StreamingContextUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

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

object RealTimeDAUApp {

    private val logger: Logger = LoggerFactory.getLogger(RealTimeDAUApp.getClass)

    val sdfDay = new SimpleDateFormat("yyyyMMdd")
    val sdfHour = new SimpleDateFormat("yyyyMMddHH")

    def main(args: Array[String]): Unit = {


        //2. 从kafka获得数据
        //kafka配置参数
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> GmallConfigs.KAKFA_BOOTSTRAP_SEVERS,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.GROUP_ID_CONFIG -> "gmall-realtime",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
            ConsumerConfig.RECEIVE_BUFFER_CONFIG -> (65536: java.lang.Integer),
            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> (500: java.lang.Integer),
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1024: java.lang.Integer)
        )
        //kafka topic:数据来源
        val topics = Array(GmallConstants.KAFKA_TOPIC_STARTUP)

        //1. 创建steamingContext
        val ssc = StreamingContextUtils.createStreamingContext("RealTimeDAUApp",
            "local[*]",
            null,
            Seconds(5))

        //2. 从kafka获取数据流
        val startDStream = KafkaUtils.getDirectStream(ssc, kafkaParams, topics)

        //3. 业务处理
        process(startDStream, dauAcc = RealTimeDAUAccumulator.getDauAccumulator(ssc))

        //4. 启动并等待sparkstreaming应用停止
        ssc.start()
        ssc.awaitTermination()
    }

    def process(startDStream: InputDStream[ConsumerRecord[String, String]], dauAcc: RealTimeDAUAccumulator) = {

        var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
        var canCommitOffsets: CanCommitOffsets = null
        val sdf = new SimpleDateFormat("yyyyMMddHH")
        val unLoginedStartupEvent = startDStream.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            canCommitOffsets = startDStream.asInstanceOf[CanCommitOffsets]
            //过滤掉已登录过的用户
            rdd.mapPartitions(iter => {
                //从redis获取今天已登录过的用户
                var dauMids = getDailyActiveMids()
                iter.map(record => {
                    val jo = JSON.parseObject(record.value())
                    StartUpEvent(jo.getString("area"),
                        jo.getIntValue("uid"),
                        jo.getString("os"),
                        jo.getString("ch"),
                        jo.getString("appid"),
                        jo.getString("mid"),
                        jo.getString("vs"),
                        jo.getLongValue("ts")
                    )
                }).filter(!havedLogin(_, dauMids))
            })
        })


        //计算日活，写到redis
        unLoginedStartupEvent.map(s => (s.mid, s.ts))
            .reduceByKey(_.min(_))
            .map(t => (sdfHour.format(new Date(t._2)), t._1))
            .foreachRDD(rdd => {
                //保存结果到reids: hset gmall_dau_hour 小时  dau
                rdd.foreachPartition(iter => {
                    val pool = RedisUtils.getPool(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
                    val jedis = pool.getResource
                    val midsKey = s"dau:${sdfDay.format(new Date())}"
                    iter.foreach(it => {
                        logger.info(s"消费数据:$it")
                        //按小时累加日活到累加器
                        dauAcc.add(it._1)
                        //把已统计的mid加入当日已活跃的redis中的set中
                        jedis.sadd(midsKey, it._2)
                    })
                    jedis.close()
                })
                //保存dau结果到redis
                val pool = RedisUtils.getPool(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
                val jedis = pool.getResource
                val dauHour = dauAcc.value
                for ((hour, dau) <- dauHour) {
                    //hset gmall_dau_hour hour dau
                    jedis.hset(GmallConstants.REDIS_DAU_HOUR_KEY, hour, dau.toString)
                }
                jedis.close()
                //提交offset
                KafkaUtils.commitOffsets(offsetRanges, canCommitOffsets)
            })
    }

    /**
      * 判断该用户是否已经登录过
      *
      * @param startUpEvent
      * @return
      */
    def havedLogin(startUpEvent: StartUpEvent, dauMids: java.util.Set[String]): Boolean = {
        dauMids.contains(startUpEvent.mid)
    }

    /**
      * 从redis中读取当天已经登陆过的用户
      *
      * @return
      */
    def getDailyActiveMids(): java.util.Set[String] = {

        val jedis = new Jedis(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
        val key = s"dau:${sdfDay.format(new Date())}"
        val exists = jedis.exists(key)
        if (!exists) {
            jedis.sadd(key, key)
            //设置该key的过期时间为明天0点
            val tomorrow = sdfDay.format(new Date(System.currentTimeMillis() + 24 * 3600000L));
            jedis.expireAt(key, sdfDay.parse(tomorrow).getTime / 1000L)
        }
        val dauMids = jedis.smembers(key)
        jedis.close()
        dauMids
    }


}
