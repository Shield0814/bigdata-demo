package com.ljl.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountAppScala {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("dd").setMaster("local[*]")

        val sc = new SparkContext(conf)

        val lines = sc.makeRDD(Array(
            "hello word count",
            "hello word count",
            "hello word count",
            "hello word count"
        ))

        val value = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

        val RES1 = value.aggregate(1)(_ + _, _ + _)
        println("RES1:" + RES1)

        val RES2 = value.treeAggregate(1)(_ + _, _ + _, 2)
        println("RES2:" + RES2)

        //        println("1" * 20)
        //        println(wordcount1(lines).collect().mkString(","))
        //
        //        println("2" * 20)
        //        println(wordcount2(lines).collect().mkString(","))
        //
        //        println("3" * 20)
        //        println(wordcount3(lines).collect().mkString(","))
        //
        //        println("4" * 20)
        //        println(wordcount4(lines).collect().mkString(","))
        //
        //        println("5" * 20)
        //        println(wordcount5(lines).collect().mkString(","))
        //
        //        println("6" * 20)
        //        println(wordcount6(lines).collect().mkString(","))
        //        println("7" * 20)
        //        println(wordcount7(lines).mkString(","))

        //        println("9" * 20)
        //        println(wordcount9(lines).mkString(","))
        //        println("10" * 20)
        //        println(wordcount10(lines).collect.mkString(","))

        sc.stop()

    }

    def wordcount1(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .reduceByKey(_ + _)
    }

    def wordcount2(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .groupBy(_._1)
            .mapValues(iter => iter.map(_._2).sum)
    }

    def wordcount3(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .foldByKey(0)(_ + _)
    }

    def wordcount4(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .aggregateByKey(0)(_ + _, _ + _)
    }

    def wordcount5(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .combineByKey[Int]((v: Int) => v, (v1: Int, v2: Int) => v1 + v2, (v1: Int, v2: Int) => v1 + v2)
    }


    def wordcount6(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .groupBy(x => x)
            .mapValues(_.size)
    }

    def wordcount7(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .aggregate(collection.mutable.Map[String, Int]())((wc, w) => {
                wc.put(w, wc.getOrElseUpdate(w, 0) + 1)
                wc
            }, (wc1, wc2) => {
                for ((w, c) <- wc2) {
                    wc1.put(w, wc1.getOrElseUpdate(w, 0) + c)
                }
                wc1
            }).toList
    }

    def wordcount8(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .countByValue()
    }

    def wordcount9(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .countByValue()
            .map(wc => (wc._1._1, wc._2))

    }

    def wordcount10(lines: RDD[String]) = {
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .combineByKeyWithClassTag(
                createCombiner = (v: Int) => v,
                mergeValue = (comb: Int, v: Int) => comb + v,
                mergeCombiners = (comb1: Int, comb2: Int) => comb1 + comb2)
    }


}
