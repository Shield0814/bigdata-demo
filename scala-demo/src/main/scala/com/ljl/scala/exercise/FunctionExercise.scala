package com.ljl.scala.exercise

import scala.collection.mutable

object FunctionExercise {


    def main(args: Array[String]): Unit = {

        //        println(productRecusive("Hello"))

        //        println(product3("Hello"))

        //        println(xn(8, -1))
        //        implicit def f(x: Any): Int = x.toString.toInt
        //
        //        def f10(arr: Array[Int], op: Int => Int) = for (ele <- arr) yield op(ele)


        //

        //        val ints = f10(Array(1, 2, 3), _ + 1)
        //        println(ints)
        //        println(f1(2))

        //        def f(x: Double) = if (x >= 0) Some(math.sqrt(x)) else None
        //
        //        def g(x: Double) = if (x != 1) Some(1 / (x - 1)) else None
        //
        //
        //        val h = compose(f, g)
        //        println(h(2))
        //        println(h(1))
        //        println(values(math.pow(_, 2), -5, 5).mkString(","))

        //        println(maxViaReduceLeft(Array(1,2,3,4,9)))
        //        println(factorial(5))

        //        println(largest(x => 10 * x - x * x, 1 to 10))

        //        println(largest1(x => 10 * x - x * x, 1 to 10))
        //        println(adjustToPair(_ * _)(6, 7))

        //        List(1,2)
        //        unless(-1 > 0){
        //            println("哈哈哈")
        //        }
        val list = List(
            ("Hello World Scala Spark", 4),
            ("Hello World Scala", 3),
            ("Hello World", 2),
            ("Hello", 1)
        )


        println("1 " + wordCount1(list))
        println("2 " + wordCount2(list))
        println("3 " + wordCount3(list))
        println("4 " + wordCount4(list))
        println("5 " + wordCount5(list))
        println("6 " + wordCount6(list))
        println("7 " + wordCount7(list))
        println("8 " + wordCount8(list))
    }

    // 实现一个unless控制抽象，工作机制类似if,但条件是反过来的
    def unless(condition: => Boolean)(op: => Unit) = {
        if (!condition) {
            op
        }
    }

    //要得到一个序列的对偶很容易，比如:
    //val pairs = (1 to 10) zip (11 to 20)
    //编写函数adjustToPair,该函数接受一个类型为(Int,Int)=>Int的函数作为参数，
    // 并返回一个等效的, 可以以对偶作为参数的函数。
    // 举例来说就是:adjustToPair(_*_)((6,7))应得到42。
    // 然后用这个函数通过map计算出各个对偶的元素之和
    def adjustToPair(f: (Int, Int) => Int) = (x: Int, y: Int) => f(x, y)


    //修改前一个函数，返回最大的输出对应的输入。
    // 举例来说,largestAt(fun:(Int)=>Int,inputs:Seq[Int])应该返回5。不得使用循环或递归
    def largest1(fun: Int => Int, inputs: Seq[Int]) = {
        val maxOutput = inputs.map(fun).max
        inputs.zip(inputs.map(fun)).filter(_._2 == maxOutput).map(_._1)
    }


    //编写函数largest(fun:(Int)=>Int,inputs:Seq[Int]),输出在给定输入序列中给定函数的最大值。
    // 举例来说，largest(x=>10x-x*x,1 to 10)应该返回25.不得使用循环或递归
    def largest(fun: Int => Int, inputs: Seq[Int]): Int = inputs.map(fun).max


    //用to和reduceLeft实现阶乘函数,不得使用循环或递归
    def factorial(n: Int) = 1 to n reduceLeft (_ * _)


    //如何用reduceLeft得到数组Array(1,333,4,6,4,4,9,32,6,9,0,2)中的最大元素?
    def maxViaReduceLeft[A <% Ordered[A]](arr: Array[A]): A = {
        arr.reduceLeft((maxV, v) => if (maxV < v) v else maxV)
    }

    //编写函数values(fun:(Int)=>Int,low:Int,high:Int),该函数输出一个集合，对应给定区间内给定函数的输入和输出。
    // 比如，values(x=>x*x,-5,5)应该产出一个对偶的集合(-5,25),(-4,16),(-3,9),…,(5,25)
    def values[B](fun: Int => B, low: Int, high: Int) = {
        for (i <- low to high) yield (i, fun(i))
    }


    //编写一个compose函数，将两个类型为Double=>Option[Double]的函数组合在一起，
    // 产生另一个同样类型的函数。如果其中一个函数返回None，则组合函数也应返回None
    def compose(f1: Double => Option[Double], f2: Double => Option[Double]): Double => Option[Double] = {
        (x: Double) =>
            (f1(x), f2(x)) match {
                case (Some(r1), Some(r2)) => Some(1)
                case _ => None
            }

    }

    //一个数字如果为正数，则它的signum为1;如果是负数,则signum为-1;如果为0,则signum为0.编写一个函数来计算这个值
    def signal(num: Int): Int = if (num > 0) 1 else if (num < 0) -1 else 0


    //编写一个过程countdown(n:Int)，打印从n到0的数字
    def countdown(n: Int): Unit = for (elem <- Range(n, -1, -1)) {
        println(elem)
    }

    //编写一个for循环,计算字符串中所有字母的Unicode代码（toLong方法）的乘积。举例来说，"Hello"中所有字符串的乘积为9415087488L
    def stringMulti(str: String): Unit = {
        var res = 1L
        for (ele <- str) {
            res *= ele.toLong
        }
        res
    }

    def stringMulti2(str: String) = {
        var res = 1L
        str.foreach((ele) => res *= ele.toLong)
        res
    }

    //编写一个函数product(s:String)，计算字符串中所有字母的Unicode代码（toLong方法）的乘积
    def product(str: String) = str.foldLeft(1L)(_ * _.toLong)

    def product1(str: String) = str./:(1L)(_ * _)

    def product2(str: String) = (1L /: str) (_ * _)

    def product3(str: String) = (str :\ 1L) (_ * _)

    def productRecusive(str: String): Long = if (str.length == 1) str(0).toLong else str.head * productRecusive(str.tail)

    //计算x的n次方
    def xn(x: Int, n: Int): Double = if (n == 0) 1 else if (n < 0) 1.0 / xn(x, -n) else x * xn(x, n - 1)


    //函数的定义

    def f3(): Unit = {

    }

    def f2(x: Int, y: Int*): Int = {
        x + y.reduce(_ + _)
    }

    def f1(x: Int): Int = {
        println(s"计算x的平方")
        math.pow(x, 2).toInt
    }


    def test(i: Int): Int = {
        if (i == 1) {
            1
        } else {
            i * test(i - 1)
        }
    }


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

    def wordCount7(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        wordCountList.aggregate(mutable.Map[String, Int]())((wcs, wc) => {
            wcs += ((wc._1, wcs.getOrElse(wc._1, 0) + wc._2))
        }, (wcs1, wcs2) => {
            for ((k, v) <- wcs2) {
                wcs1 += ((k, wcs1.getOrElse(k, 0) + v))
            }
            wcs1
        }).toList.sortBy(-_._2).take(3)
    }

    def wordCount8(list: List[(String, Int)]) = {
        val wordCountList = list.flatMap(t => t._1.split("\\W").map((_, t._2)))
        (wordCountList :\ mutable.Map[String, Int]()) ((wc, wcs) => wcs += ((wc._1, wcs.getOrElse(wc._1, 0) + wc._2)))
            .toList.sortBy(-_._2).take(3)
    }

    def tests() = {
        val map1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
        val map2 = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)

        for ((k, v) <- map1) {
            map2 += ((k, map2.getOrElse(k, 0) + v))
        }
        map2
    }
}
