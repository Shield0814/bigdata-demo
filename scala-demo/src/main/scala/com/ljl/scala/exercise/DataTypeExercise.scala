package com.ljl.scala.exercise

import java.util
import java.util.TimeZone

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object DataTypeExercise extends App {

    //    exer1(10)

    //    exer2(Array(1, 2, 3, 4, 5))

    //    exer3(Array(1,2,-1,-3,3))

    //    exer5()

    //    println(wordcount("alert spark hadoop hello world ha ha ha"))
    //    println(wordcount2("alert spark hadoop hello world ha ha ha"))
    //    println(wordcount3("alert spark hadoop hello world ha ha ha"))

    //        println(indexes("Mississippi"))

    //    println(removeZero(List(1, 2, 3, 0, 0, 1, 0)))

    //    println(exer13(Array("Tom", "Fred", "Harry"), Map("Tom" -> 3, "Dick" -> 4, "Harry" -> 5)))

    //    println(List(1, 2, 3).mkString(","))
    //    println(exer14(List(1, 2, 3), ","))

    //    println(reverseList(List(1, 2, 3)))
    //    println("="*29)
    //    println(reverseList2(List(1, 2, 3)))

    val lines = List(
        "scala version word count",
        "scala version word count",
        "flink version word "
    )
    //    println(wcPar(lines))

    println(wcPar2(lines))


    def wcPar2(lines: Seq[String]) = {
        lines.par.flatMap(_.split("\\W"))
            .map((_, 1))
            .groupBy(_._1)
            .aggregate(ListBuffer[(String, Int)]())((res, wcs) => {
                val count = wcs._2.map(_._2).sum
                res.append((wcs._1, count))
                res
            }, (com1, com2) => {
                com1.appendAll(com2)
                com1
            })

    }

    def wcPar1(lines: List[String]): mutable.Map[String, Int] = {
        lines.par.flatMap(_.split("\\W"))
            .aggregate(mutable.Map[String, Int]())((res, word) => {
                res.put(word, res.getOrElse(word, 0) + 1)
                res
            }, (res1, res2) => {
                //(res1 /: res2)((res,element) => res.updated(element._1,res.getOrElse(element._1,0) + element._2))
                for ((k, v) <- res1) {
                    res2.put(k, res2.getOrElse(k, 0) + v)
                }
                res2
            })
    }

    //通过par编写一个并行的wordcount程序，统计字符串中每个字符的出现频率。可以通过并行集合的aggregate函数试试
    def wcPar(lines: List[String]): Map[String, Int] = {
        lines.par.flatMap(_.split("\\W"))
            .aggregate(Map[String, Int]())((res, word) => {
                res + (word -> (res.getOrElse(word, 0) + 1))
            }, (res1, res2) => {
                (res1 /: res2) ((res, element) => res.updated(element._1, res.getOrElse(element._1, 0) + element._2))
                //                for ((k,v) <- res1){
                //                    res2.updated(k,res2.getOrElse(k,0) + v)
                //                }
                //                res2
            })
    }

    //编写一个函数，将Double数组转换成二维数组。传入列数作为参数。
    // eg，传入Array(1,2,3,4,5,6)和3列，返回Array(Array(1,2,3), Array(4,5,6))
    def to2Dim[T](arr: Array[T], colSize: Int): Array[Array[T]] = {

        null
    }


    //给定整型列表lst，(lst :\ List[Int]())(_ :: _)得到什么?-----
    // (List[Int]() /: lst)(_ :+ _)又得到什么？
    // 如何修改他们中的一个，以对原列表进行反向排列?
    //右折叠:从右向左遍历
    def reverseList[T](lst: List[T]): List[T] = (lst :\ List[T]()) ((ele, res) => {
        println(ele)
        res :+ ele
    })

    //左折叠：从左向右遍历
    def reverseList2[T](lst: List[T]): List[T] = (List[T]() /: lst) ((res, ele) => {
        println(ele)
        res :+ ele
    })

    //实现一个函数，作用与mkStirng相同，提示：使用reduceLeft实现试试
    def exer14(seq: Seq[Int], delimiter: String) = seq.map(_.toString).reduceLeft(_ + delimiter + _)


    //编写一个函数，接受一个字符串的集合，以及一个从字符串到整数值的映射。
    // 返回整形的集合，其值为能和集合中某个字符串相对应的映射的值。
    //举例来说，给定Array(“Tom”,”Fred”,”Harry”)和Map(“Tom”->3,”Dick”->4,”Harry”->5)，返回Array(3,5)。
    // 提示：用flatMap将get返回的Option值组合在一起
    def exer13(seq: Seq[String], map: Map[String, Int]): Seq[Int] = {
        val res = ListBuffer[Int]()
        seq.foreach {
            map.get(_) match {
                case Some(value) =>
                    res += value
                case None =>
            }
        }
        res
    }

    //编写一个函数，从一个整型链表中去除所有的零值
    def removeZero(list: List[Int]): List[Int] = list.filter(_ != 0)


    //编写一个函数indexes，给定字符串，产出一个包含所有字符下标的映射。
    // 举例来说：indexes(“Mississippi”)应返回一个映射，让’M’对应集{0}，‘i’对应集{1，4，7，10}，依次类推。
    // 使用字符到可变集的映射，注意下标的集应该是有序的。
    def indexes(str: String): Map[Char, ArrayBuffer[Int]] = {
        var i = 0
        (Map[Char, ArrayBuffer[Int]]() /: str) ((r, c) => {
            r.get(c) match {
                case Some(idxs) =>
                    idxs += i
                    i += 1
                    r.updated(c, idxs)
                case None =>
                    val idxs = ArrayBuffer(i)
                    i += 1
                    r.updated(c, idxs)

            }
        })
    }

    //编写一个函数 minmax(values:Array[Int]), 返回数组中最小值和最大值的对偶
    def minmax(values: Array[Int]): (Int, Int) = (values.min, values.max)


    //3. 给定一个整数数组，产出一个新的数组，包含原数组中的所有正值，以原有顺序排列，之后的元素是所有零或负值，以原有顺序排列
    def exer3(arr: Array[Int]): Unit = {
        val positiveArr = ArrayBuffer[Int]()
        val negtiveArr = ArrayBuffer[Int]()

        arr.foreach(ele =>
            if (ele > 0) {
                positiveArr.append(ele)
            } else {
                negtiveArr.append(ele)
            }
        )
        positiveArr.appendAll(negtiveArr)
        println(positiveArr.toArray.mkString(","))
    }

    //创建一个由java.util.TimeZone.getAvailableIDs返回的时区集合，并只显示以America/前缀开头的时区，并且有序。
    def exer4(): Unit = {
        TimeZone.getAvailableIDs.filter(_.startsWith("America/")).sorted.foreach(println)
    }

    //5、设置一个映射，其中包含你想要的一些装备，以及它们的价格。然后根据这个映射构建另一个新映射，采用同一组键，但是价格上打9折。
    def exer5(): Unit = {
        val equipment = Map("无尽战刃" -> 2300, "暗影战斧" -> 1800)
        val afterDiscount = equipment.mapValues(_ * 0.9)
        println(afterDiscount)
    }

    //6、编写一段WordCount函数，统计传入的字符串中单词的个数
    def wordcount(str: String): Predef.Map[String, Int] =
        (Map[String, Int]() /: str.split("\\W")) ((res, word) => res.updated(word, res.getOrElse(word, 0) + 1))


    //7、重复上一个练习，使统计后的单词有序
    def wordcount2(str: String): Map[String, Int] = wordcount(str).toSeq.sortBy(_._1).toMap

    //8、 重复前一个练习，使用java.util.TreeMap进行实现，并使之适用于Scala API
    def wordcount3(str: String): java.util.TreeMap[String, Int] = {
        (new util.TreeMap[String, Int]() /: str.split("\\W")) ((res, word) => {
            res.put(word, res.getOrDefault(word, 0) + 1)
            res
        })
    }


    //编写一个循环，将整数数组中相邻的元素置换。比如Array(1, 2, 3, 4, 5)置换后为Array(2, 1, 4, 3, 5)
    def exer2[T <% Comparable[T]](arr: Array[T]): Unit = {
        println(s"交换前:${arr.mkString(",")}")
        var tmp: T = arr(0)
        for (i <- Range(0, arr.length, 2) if i < arr.length - 1) {
            tmp = arr(i)
            arr(i) = arr(i + 1)
            arr(i + 1) = tmp;
        }
        println(s"交换后:${arr.mkString(",")}")
    }


    //编写一段代码，将a设置为一个n个随机整数的数组，要求随机数介于0和n之间
    def exer1(n: Int): Unit = {
        val arr = new Array[Int](n)
        for (i <- 0 until n) {
            arr(i) = (math.random * n).toInt
        }
        println(arr.mkString(","))

    }
}
