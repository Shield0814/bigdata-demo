package com.ljl.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//用户点击广告操作
case class UserAdClickAction(timestamp: Long, province: Int, city: Int, userId: Int, adId: Int)

object ProvinceAdClickTopNApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            //            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //            .registerKryoClasses(Array(classOf[UserAdClickAction]))
            .setAppName("ProvinceAdClickTopNApp")
            .setMaster("local[1]")

        val sc = new SparkContext(conf)

        //用户点击广告行为数据
        val userAddClickActions = sc.textFile(args(0)).map(record => {
            val fields = record.split("\\W")
            UserAdClickAction(fields(0).toLong, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toInt)
        })

        //每个省份各个城市的广告点击量
        val provinceCityAdClickCount = userAddClickActions.map(uac => ((uac.province, uac.city), 1))
            .reduceByKey(_ + _)

        provinceCityAdClickCount.persist(StorageLevel.MEMORY_ONLY_2)
        val top3_1 = computeProvinceAdClickCountTop3City(provinceCityAdClickCount)
        top3_1.saveAsTextFile(s"${args(1)}/1")
        println("=" * 20)
        val top3_2 = compute2ProvinceAdClickCountTop3City(provinceCityAdClickCount)
        top3_2.saveAsTextFile(s"${args(1)}/2")
        println("=" * 20)
        val top3_3 = compute3ProvinceAdClickCountTop3City(provinceCityAdClickCount)
        top3_3.saveAsTextFile(s"${args(1)}/3")

        Thread.sleep(Int.MaxValue)
        sc.stop()
    }


    /**
      * 计算各省广告点击次数top3的城市,通过groupby实现
      *
      * @param provinceCityAdClickCount
      * @return
      */
    def computeProvinceAdClickCountTop3City(provinceCityAdClickCount: RDD[((Int, Int), Int)]) = {
        val provinceCityAdClickTop3 = provinceCityAdClickCount.groupBy(_._1._1)
            .mapValues(iter => {
                iter.toList.sortWith(_._2 > _._2)
                    .map(cityClickCount => (cityClickCount._1._2, cityClickCount._2))
                //                    .take(3)
            })
        provinceCityAdClickTop3
    }

    /**
      * 计算各省广告点击次数top3的城市,通过 aggregateByKey 实现
      *
      * @param provinceCityAdClickCount
      * @return
      */
    def compute2ProvinceAdClickCountTop3City(provinceCityAdClickCount: RDD[((Int, Int), Int)]) = {
        provinceCityAdClickCount.map(r => (r._1._1, (r._1._2, r._2)))
            .aggregateByKey(ListBuffer[(Int, Int)]())((ptop3, cityCount) => {
                ptop3.append(cityCount)
                if (ptop3.size > 3) {
                    ptop3.sortWith(_._2 > _._2).take(3)
                } else {
                    ptop3
                }
            }, (p1Top3, p2Top3) => {
                p1Top3.appendAll(p2Top3)
                p1Top3.sortWith(_._2 > _._2).take(3)
            })
    }

    /**
      * 计算各省广告点击次数top3的城市,通过 combineByKey 实现
      *
      * @param provinceCityAdClickCount
      * @return
      */
    def compute3ProvinceAdClickCountTop3City(provinceCityAdClickCount: RDD[((Int, Int), Int)]) = {

        val createCombiner = (v: (Int, Int)) => ListBuffer[(Int, Int)](v)
        val mergeValue = (comb: ListBuffer[(Int, Int)], v: (Int, Int)) => {
            comb.append(v)
            if (comb.size > 3) comb.sortWith(_._2 > _._2).take(3)
            else comb
        }
        val mergeCombiner = (comb1: ListBuffer[(Int, Int)], comb2: ListBuffer[(Int, Int)]) => {
            comb1.appendAll(comb2)
            comb1.sortWith(_._2 > _._2).take(3)
        }
        provinceCityAdClickCount.map(r => (r._1._1, (r._1._2, r._2)))
            .combineByKey(createCombiner, mergeValue, mergeCombiner)

        //combineByKey使用注意事项：combineByKey的泛型最好指定
        //        val value: RDD[(Int, (Int, Int))] = provinceCityAdClickCount.map(r => (r._1._1, (r._1._2, r._2)))
        //        value.combineByKey[ListBuffer[(Int, Int)]](
        //            (v:(Int, Int)) => ListBuffer(v),
        //            (comb, v:(Int, Int)) => {
        //                comb.append(v)
        //                if (comb.size > 3) comb.sortWith(_._2 > _._2).take(3)
        //                else comb
        //            }, (comb1, comb2) => {
        //                comb1.appendAll(comb2)
        //                comb1.sortWith(_._2 > _._2).take(3)
        //            })
    }
}
