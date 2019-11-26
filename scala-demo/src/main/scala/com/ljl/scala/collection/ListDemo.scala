package com.ljl.scala.collection

import scala.collection.mutable

object ListDemo {

    def main(args: Array[String]): Unit = {


        val list = (1 to 10).toList
        //== list.foldLeft(1)(_*_)
        val product = list.product
        println(product)

        //切片，获得一个[2,5)的子list集合
        val ints = list.slice(2, 5)
        println(ints)

        //滑动窗口
        val iter = list.sliding(3, 1)
        while (iter.hasNext) {
            println(iter.next().sum)
        }
        list.sliding(3, 1).foreach(println)


    }

    def wordcount1(): Unit = {
        val list = List(
            "hello world,hello world,map reduce,hello",
            "map reduce,dfs hello,world"
        )
        list.flatMap(_.split("[, ]")).map((_, 1))
            .groupBy(_._1)
            .mapValues(list => list.map(_._2).sum)
            .foreach(println)

    }

    /**
      * 统计词频，并按出现次数降序排序
      */
    def wordcount(): Unit = {
        val list = List(
            "hello world,hello world,map reduce,hello",
            "map reduce,dfs hello,world"
        )

        val res = mutable.Map[String, Int]()

        list.flatMap(_.split("[, ]"))
            .foldLeft(res)((res, word) => {
                if (res.contains(word)) {
                    res += (word -> (res(word) + 1))
                } else {
                    res += (word -> 1)
                }
            }).toSeq.sortBy(-_._2)
            .foreach(println)

        println("=========================")
        val stringToTuples = list.flatMap(_.split("[, ]"))
            .map((_, 1))
            .groupBy(_._1)
        stringToTuples.map(x => (x._1, x._2.size))
            .toSeq.sortBy(-_._2)
            .foreach(println)


    }

}
