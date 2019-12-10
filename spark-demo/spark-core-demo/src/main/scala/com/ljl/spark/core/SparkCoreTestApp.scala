package com.ljl.spark.core

import java.time.Instant

import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreTestApp {

    def main(args: Array[String]): Unit = {
        val start = Instant.now()
        val conf = new SparkConf()
            .setAppName("SparkCoreTestApp")
            .setMaster("local[1]")
            .set("spark.default.parallelism", "3")
            .set("spark.executor.memory", "6G")
            .set("spark.files.maxPartitionBytes", "268435456")
            .set("dfs.blocksize", "268435456")
        val sc = new SparkContext(conf)


        println("开始毫秒数: " + start.getEpochSecond)

        sc.textFile("file:///d:/data/spark-test", 1).coalesce(1)
            .repartition(1)
            .flatMap(_.split("\\W"))
            .map((_, 1))
            .reduceByKey(_ + _)
            .saveAsTextFile("file:///d:/data/out")

        sc.stop()
        val end = Instant.now()
        println("结束毫秒数: " + end.getEpochSecond)
        println(s"总耗时: ${end.getEpochSecond - start.getEpochSecond}")
    }
}
