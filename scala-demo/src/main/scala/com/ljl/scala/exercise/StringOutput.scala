package com.ljl.scala.exercise

import scala.collection.mutable

object StringOutput extends App {

    val sql =
        """select
          |     d.deptno,
          |     max(e.sal + nvl(e.comm,0)) as max_sal,
          |     sum(e.sal + nvl(e.comm,0)) as total_sal
          |from dept d
          |left join emp e on d.deptno = e.deptno
          |where d.deptno = ?
        """.stripMargin

    val sql2 = s"select * from ($sql) t"


    val name = "ddd"
    val age = 10
    val sal = 1.293232

    printf("name=%s,age=%5d,sal=%.2f", name, age, sal)


    println(s"name=$name,age=$age,sal=$sal")

    val list = List(
        ("Hello World Scala Spark", 4),
        ("Hello World Scala", 3),
        ("Hello World", 2),
        ("Hello", 1)
    )


    println(wordCount1(list))
    println(wordCount2(list))
    println(wordCount3(list))
    println(wordCount4(list))
    println(wordCount5(list))
    println(wordCount6(list))

    def wordCount1(list: List[(String, Int)]) = {
        list.flatMap {
            case (line, count) =>
                line.split("\\W").map((_, count))
        }.groupBy(_._1).map {
            case (word, wcs) =>
                (word, wcs.map(_._2).sum)
        }.toList.sortBy(-_._2).take(3)
    }

    def wordCount2(list: List[(String, Int)]) = {
        list.flatMap(t => t._1.split("\\W").map((_, t._2)))
            .groupBy(_._1)
            .map(wcs => (wcs._1, wcs._2.map(_._2).sum))
            .toList.sortBy(-_._2).take(3)
    }

    def wordCount3(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        wordCountList.foldLeft(mutable.Map[String, Int]())((wcs, wc) => {
            wcs.put(wc._1, wcs.getOrElse(wc._1, 0) + wc._2)
            wcs
        }).toList.sortBy(-_._2).take(3)
    }

    def wordCount4(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        (mutable.Map[String, Int]() /: wordCountList) ((wcs, wc) => {
            wcs.put(wc._1, wcs.getOrElse(wc._1, 0) + wc._2)
            wcs
        }).toList.sortBy(-_._2).take(3)

    }

    def wordCount5(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        (mutable.Map[String, Int]() /: wordCountList) ((wcs, wc) => wcs += ((wc._1, wcs.getOrElse(wc._1, 0) + wc._2))).toList.sortBy(-_._2).take(3)
    }

    def wordCount6(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        wordCountList.par.aggregate(mutable.Map[String, Int]())((wcs, wc) => {
            wcs += ((wc._1, wcs.getOrElse(wc._1, 0) + wc._2))
        }, (wcs1, wcs2) => {
            for ((k, v) <- wcs2) {
                wcs1 += ((k, wcs1.getOrElse(k, 0) + v))
            }
            wcs1
        }).toList.sortBy(-_._2).take(3)
    }
}
