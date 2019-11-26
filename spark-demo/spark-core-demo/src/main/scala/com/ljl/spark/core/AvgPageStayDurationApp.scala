package com.ljl.spark.core

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 用户访问动作表
  *
  * @param date               用户点击行为的日期
  * @param user_id            用户的ID
  * @param session_id         Session的ID
  * @param page_id            某个页面的ID
  * @param action_time        动作的时间点
  * @param search_keyword     用户搜索的关键词
  * @param click_category_id  某一个商品品类的ID
  * @param click_product_id   某一个商品的ID
  * @param order_category_ids 一次订单中所有品类的ID集合
  * @param order_product_ids  一次订单中所有商品的ID集合
  * @param pay_category_ids   一次支付中所有品类的ID集合
  * @param pay_product_ids    一次支付中所有商品的ID集合
  * @param city_id            城市 id
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

case class CategoryStatInfo(categoryId: Long, clickCnt: Long, orderCnt: Long, payCnt: Long)

object AvgPageStayDurationApp {

    //    val logger: Logger = LoggerFactory.getLogger(AvgPageStayDurationApp.getClass)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("AvgPageStayDurationApp").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val path = "D:\\projects\\bigdata-demo\\spark-demo\\spark-core-demo\\src\\main\\resources\\user_visit_action.txt"
        //2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3

        //用户访问日志
        val userVisitActionRDD = sc.textFile(path).map(line => {
            val sps = line.split("_")
            UserVisitAction(sps(0), sps(1).toLong, sps(2), sps(3).toLong,
                sps(4), sps(5), sps(6).toLong, sps(7).toLong, sps(8), sps(9), sps(10), sps(11), sps(12).toLong)
        })

        //需求一：计算页面平均停留时长
        val avgPageStayDurationRDD = computeAvgPageStayDuration(sc, userVisitActionRDD)
        avgPageStayDurationRDD.foreach(println)

        userVisitActionRDD.flatMap {
            case uva: UserVisitAction if uva.click_category_id != -1 =>
                Iterator(CategoryStatInfo(uva.click_category_id, 1L, 0L, 0L))
            case uva: UserVisitAction if uva.order_category_ids != "null" =>
                uva.order_category_ids.split(",")
                    .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 1L, 0L))
                    .toIterator
            case uva: UserVisitAction if uva.pay_category_ids != "null" =>
                uva.pay_category_ids.split(",")
                    .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 0L, 1L))
                    .toIterator
            case _ =>
                Iterator()
        }

        sc.stop()
    }

    /**
      * 计算页面平均停留时长
      *
      * @param userVisitActionRDD
      */
    def computeAvgPageStayDuration(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //1. 计算每个session中用户在每个页面的停留时长
        //        (sessionId,(pageId,stayTime))
        val sessionPageStayDuration = userVisitActionRDD
            .map(uva => (uva.session_id, (uva.page_id, sdf.parse(uva.action_time).getTime)))
            .groupByKey()
            .flatMapValues(pageActions => {
                //                List((pageId,actionTime))
                val visitPageFlow = pageActions.toList.sortWith(_._2 < _._2)
                //某个session中，用户在某个页面的停留时长
                val pageStayDuration = visitPageFlow.zip(visitPageFlow.tail).map {
                    case (page1, page2) => (page1._1, page2._2 - page1._2)
                }
                pageStayDuration
            })

        //缓存每个session中用户在每个页面的停留时间
        sessionPageStayDuration.cache()


        //2. 计算每个页面用户总的停留时长
        val pageTotalStayDuration = sessionPageStayDuration.map(_._2).reduceByKey(_ + _)

        //3. 计算每个页面用户的访问次数
        val pageViewCount = sessionPageStayDuration.map(_._2).countByKey()

        //广播页面访问次数
        val pvBroadcast = sc.broadcast(pageViewCount)
        println("&&&&&&&&&&&&&&&&&&" + pageViewCount.values.sum)

        //4. 计算页面平均停留时长
        pageTotalStayDuration.map {
            case (pageId, stayDuration) =>
                if (!pvBroadcast.value.contains(pageId))
                    println(s"页面:$pageId 不存在，计算可能出现了错误")
                (pageId, stayDuration.toDouble / pvBroadcast.value.getOrElse(pageId, 1L))
        }

    }
}
