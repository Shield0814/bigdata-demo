package com.ljl.spark.core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

object WordCountViaAccumulatorApp {

    def main(args: Array[String]): Unit = {
        //1. 创建sparkcontext
        val conf = new SparkConf()
            .setAppName("WordCountViaAccumulatorApp")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        //2.注册累加器
        val wcAcc = new WCAccumlator
        sc.register(wcAcc, "wordCountAcc")


        //3.计算wordcount
        computeWordCountViaAcc(sc, args(0), wcAcc)

        println(sc.textFile(args(0)).partitions.length)

        //4.停止sparkcontext
        sc.stop()
    }


    /**
      * 计算wordcount
      *
      * @param sc
      * @param path
      * @param wcAcc
      */
    def computeWordCountViaAcc(sc: SparkContext, path: String, wcAcc: WCAccumlator): Unit = {
        sc.textFile(path)
            .flatMap(_.split("\\W"))
            .map(wcAcc.add)
            .count()
        println(wcAcc.value)
    }
}

//自定义计算wordcount的累加器
class WCAccumlator extends AccumulatorV2[String, Map[String, Long]] {

    var wcAcc: Map[String, Long] = Map[String, Long]()

    override def isZero: Boolean = wcAcc.isEmpty

    override def copy(): AccumulatorV2[String, Map[String, Long]] = {
        val copyWCAcc = new WCAccumlator
        wcAcc.synchronized {
            copyWCAcc.wcAcc = wcAcc.clone()
        }
        copyWCAcc
    }

    override def reset(): Unit = wcAcc.clear()

    override def add(v: String): Unit = {
        wcAcc(v) = wcAcc.getOrElseUpdate(v, 0) + 1
    }

    override def merge(other: AccumulatorV2[String, Map[String, Long]]): Unit = {
        if (other.isInstanceOf[WCAccumlator]) {
            val o = other.asInstanceOf[WCAccumlator]
            for ((word, count) <- o.wcAcc) {
                // wcAcc(word) = wcAcc.getOrElse(word,0L) + count
                wcAcc(word) = wcAcc.getOrElseUpdate(word, 0) + count
            }
        } else {
            throw new IllegalArgumentException(s"期望一个 WCAccumlator 类型的累加器，" +
                s"实际接收到的类型为 ${other.getClass}")
        }
    }

    override def value: Map[String, Long] = wcAcc
}