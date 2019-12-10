package com.sitech.gmall.realtime

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.function.Consumer

import com.alibaba.fastjson.JSON
import com.sitech.gmall.common.GmallConstants
import com.sitech.gmall.config.GmallConfigs
import com.sitech.gmall.realtime.base.ESDocumentBase
import com.sitech.gmall.realtime.util.{JestUtils, KafkaUtils, RedisUtils, StreamingContextUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.json4s.native.Serialization
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

//订单和用户信息
case class OrderUserInfo(order_id: String,
                         user_id: String,
                         order_status: String,
                         create_time: String,
                         user_gender: String,
                         user_age: Int,
                         user_level: String) {
    def mergeDetail(orderDetail: OrderDetail, dt: String) = {
        SaleDetail(orderDetail.order_detail_id,
            orderDetail.order_id,
            this.order_status,
            this.create_time,
            this.user_id,
            this.user_gender,
            this.user_age,
            this.user_level,
            orderDetail.sku_id,
            orderDetail.sku_name,
            orderDetail.sku_price.toDouble,
            dt
        )
    }
}

//订单详情信息
case class OrderDetail(order_detail_id: String,
                       order_id: String,
                       sku_id: String,
                       sku_name: String,
                       sku_price: String)


/**
  * 商品销售详情
  *
  * @param order_detail_id 商品销售id
  * @param order_id        关联订单id
  * @param order_status    订单状态
  * @param create_time     创建时间
  * @param user_id         购买用户
  * @param user_gender     用户性别
  * @param user_age        用户年龄
  * @param user_level      用户等级
  * @param sku_id          商品id
  * @param sku_name        商品名称
  * @param sku_price       商品价格
  * @param dt
  */
case class SaleDetail(var order_detail_id: String,
                      var order_id: String,
                      var order_status: String,
                      var create_time: String,
                      var user_id: String,
                      var user_gender: String,
                      var user_age: Int,
                      var user_level: String,
                      var sku_id: String,
                      var sku_name: String,
                      var sku_price: Double,
                      var dt: String) extends ESDocumentBase(id = order_detail_id) {

}

object RealTimeOrderInfoApp {

    private val logger: Logger = LoggerFactory.getLogger(RealTimeOrderInfoApp.getClass)
    private val df: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

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
        val orderInfoTopic = Array(GmallConstants.KAFKA_TOPIC_NEW_ORDER)
        val orderDetailTopic = Array(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL)

        val ssc = StreamingContextUtils.createStreamingContext(
            "RealTimeOrderInfoApp",
            "local[*]",
            null,
            Seconds(5))

        val orderRawDStream = KafkaUtils.getDirectStream(ssc, kafkaParams, orderInfoTopic)

        val orderDetailRawDStream = KafkaUtils.getDirectStream(ssc, kafkaParams, orderDetailTopic)


        //处理业务
        process(orderRawDStream, orderDetailRawDStream)

        StreamingContextUtils.startStreamingApplication(ssc, null)

        StreamingContextUtils.stopGracefully(ssc, null)

    }


    /**
      * 处理业务--订单实时处理
      * 实时监控订单数据，每收到一个下单请求，把订单详情，订单信息，用户信息进行关联形成宽表，
      * 把结果存储到 ElasticSearch 中，从而方便灵活分析
      *
      * @param orderRawDStream       实时订单流
      * @param orderDetailRawDStream 实时订单详情流
      */
    def process(orderRawDStream: InputDStream[ConsumerRecord[String, String]],
                orderDetailRawDStream: InputDStream[ConsumerRecord[String, String]]) = {
        var orderInfoOffsetRanges = Array.empty[OffsetRange]
        var orderDetailOffsetRanges = Array.empty[OffsetRange]
        var canCommitOffsets: CanCommitOffsets = null

        //订单数据流
        val orderInfoDStream = orderRawDStream.transform { rdd =>
            orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            canCommitOffsets = orderRawDStream.asInstanceOf[CanCommitOffsets]
            rdd.mapPartitions(iter => {
                val pool = RedisUtils.getPool(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
                val jedis = pool.getResource
                //                (订单id，订单及用户信息)
                val userOrder = iter.map(record => {
                    val jo = JSON.parseObject(record.value())
                    val userJsonStr = jedis.get(jo.getString(s"user_info:${jo.getString("user_id")}"))
                    val userJo = JSON.parseObject(userJsonStr)
                    val birthday = LocalDate.from(df.parse(userJo.getString("birthday")))
                    (jo.getString("order_id"),
                        OrderUserInfo(jo.getString("order_id"),
                            jo.getString("user_id"),
                            jo.getString("order_status"),
                            jo.getString("create_time"),
                            userJo.getString("user_gender"),
                            LocalDate.now().getYear - birthday.getYear,
                            userJo.getString("user_level"))
                    )
                })
                jedis.close()
                pool.close()
                userOrder
            })
        }

        //订单详情数据流
        val orderDetailDStream = orderDetailRawDStream.transform { rdd =>
            orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            //(订单id，订单详情)
            rdd.map(record => {
                val jo = JSON.parseObject(record.value())
                (jo.getString("order_id"),
                    OrderDetail(jo.getString("order_detail_id"),
                        jo.getString("order_id"),
                        jo.getString("sku_id"),
                        jo.getString("sku_name"),
                        jo.getString("sku_price"))
                )
            })
        }
        //订单流和订单详情流join
        orderInfoDStream.join(orderDetailDStream).foreachRDD { rdd =>
            rdd.foreachPartition(iter => {
                implicit val formats = org.json4s.DefaultFormats
                val pool = RedisUtils.getPool(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
                val jedis = pool.getResource
                val saleDetails = ListBuffer.empty[SaleDetail]
                iter.foreach {
                    case (orderId, (orderUserInfo, orderDetail)) =>
                        if (orderUserInfo != null) {
                            //把订单信息写到redis中，并设置20s的过期时间，防止订单详情迟到：sadd gmall_new_order 订单及用户信息json串
                            jedis.setex(s"order_info:$orderId", 20, Serialization.write(orderUserInfo))

                            //如果订单及用户信息不为空，先把订单及用户信息写到redis防止该订单的订单详情迟到的情况
                            // 则判断订单详情是否为空
                            if (orderDetail != null) {
                                // 若不为空合并信息添加到待写入es的list中
                                val saleDetail = orderUserInfo.mergeDetail(orderDetail, df.format(LocalDate.now))
                                saleDetails.append(saleDetail)
                            } else {
                                // 若为空，在redis中查缓存，看是否有存在的订单详情信息，若有，合并信息，
                                // smembers order_detail:订单id
                                val orderDetails = jedis.smembers(s"order_detail:$orderId")
                                if (!orderDetails.isEmpty) {
                                    orderDetails.forEach(new Consumer[String]() {
                                        override def accept(t: String): Unit = {
                                            //早到的orderdetail信息
                                            val oldOrderDetail = JSON.parseObject[OrderDetail](t, OrderDetail.getClass)
                                            val oldSaleDetail = orderUserInfo.mergeDetail(oldOrderDetail, df.format(LocalDate.now()))
                                            saleDetails.append(oldSaleDetail)
                                        }
                                    })
                                } else {
                                    logger.warn(s"订单${orderId}的订单详情信息迟到")
                                }
                            }

                        } else {
                            //如果订单及用户信息为空，则订单详情信息一定不为空
                            if (jedis.exists(s"order_info:$orderId")) {
                                //若redis中存在相应的订单信息，进行merge
                                val prevOrderInfo = jedis.get(s"order_info:$orderId")
                                val prevOrderUserInfo = JSON.parseObject[OrderUserInfo](prevOrderInfo, OrderUserInfo.getClass)
                                val detail = prevOrderUserInfo.mergeDetail(orderDetail, df.format(LocalDate.now()))
                                saleDetails.append(detail)
                            } else {
                                //否则写到redis中等待迟到的orderInfo信息,120s后未等到则取消
                                jedis.sadd(s"order_detail:$orderId", Serialization.write(orderDetail))
                                jedis.expire(s"order_detail:$orderId", 120)
                            }
                        }
                }
                jedis.close()
                pool.close()
                val factory = JestUtils.getJestClientFactory("bigdata116:9200,bigdata117:9200,bigdata118:9200")
                val jest = factory.getObject
                JestUtils.putDocViaBulk(jest, GmallConstants.ES_INDEX_SALE_DETAIL, saleDetails, 1000)
                jest.close()
            })
            KafkaUtils.commitOffsets(orderInfoOffsetRanges, canCommitOffsets)
            KafkaUtils.commitOffsets(orderDetailOffsetRanges, canCommitOffsets)
        }


    }
}
