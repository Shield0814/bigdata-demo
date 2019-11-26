package com.ljl.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreTestApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkCoreTestApp").setMaster("local[*]")
        val sc = new SparkContext(conf)


        println(sc.makeRDD(Array(1, 2, 3, 4)).aggregate(10)(_ + _, _ + _))


        sc.stop()
    }
}
