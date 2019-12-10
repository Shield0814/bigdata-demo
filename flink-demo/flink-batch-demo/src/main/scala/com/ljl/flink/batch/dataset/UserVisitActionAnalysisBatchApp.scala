package com.ljl.flink.batch.dataset

import java.text.SimpleDateFormat

import org.apache.flink.api.scala.{DataSet, _}
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: Long,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

/**
  * 品类维度统计信息
  *
  * @param categoryId 品类id
  * @param clickCnt   点击次数
  * @param orderCnt   下单次数
  * @param payCnt     支付次数
  */
case class CategoryStatInfo(categoryId: Long, clickCnt: Long, orderCnt: Long, payCnt: Long)

import org.apache.flink.api.scala.ExecutionEnvironment

object UserVisitActionAnalysisBatchApp {

    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val path = "D:\\projects\\bigdata-demo\\flink-demo\\flink-stream-demo\\src\\main\\resources\\user_visit_action.txt"
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val userVisitActions = env.readTextFile(path).map(line => {
            val fields = line.split("_")
            val sps = line.split("_")
            UserVisitAction(sps(0), sps(1).toLong, sps(2), sps(3).toLong,
                sdf.parse(sps(4)).getTime, sps(5), sps(6).toLong, sps(7).toLong,
                sps(8), sps(9), sps(10), sps(11), sps(12).toLong)
        })

        val categoryInfo: DataSet[CategoryStatInfo] = userVisitActions.flatMap(uva => {
            uva match {
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
        })
    }
}
