package com.ljl.scala.collection

import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

object ArrayDemo {

    def main(args: Array[String]): Unit = {

        //        fixedLengthArray()
        //        unfixedLengthArray()
        //
        //        fixedLengthArray_unfixedLengthArray_transform()
        //
        //        mdimensionArray()
        scalaArray_javaList_transform()


        val ints = Array[Int](1, 2, 3)

    }


    /**
      * scala数组ArrayBuffer 转javaList
      * 使用scala隐式转换
      */
    def scalaArray_javaList_transform(): Unit = {
        val scalaBuffer = ArrayBuffer(3, 4, 5, 1, 2)

        println("buffer 转java list前： " + scalaBuffer)

        import scala.collection.JavaConversions._
        /**
          * scala ArrayBuffer中没有sort方法，
          * java List中有sort方法，如果scala buffer想要用sort方法，
          * 就必须把scala ArrayBuffer转换成JAVA List
          */
        scalaBuffer.sort(new Comparator[Int] {
            override def compare(o1: Int, o2: Int): Int = o1 - o2
        })
        println("buffer 转java list后： " + scalaBuffer)


    }

    /**
      * 多维数组
      */
    def mdimensionArray(): Unit = {
        val mdimArr = Array.ofDim[Int](3, 4)
        for (row <- mdimArr) {
            for (col <- row) {
                print(s"$col, ")
            }
            println()
        }
    }

    /**
      * 定长数组与变长数组的转换
      * 特点：
      * 转换后原来的定长或变长数组属性不变，即：变长仍然是变长，定长仍然时定长
      */
    def fixedLengthArray_unfixedLengthArray_transform(): Unit = {
        println("===============定长数组与变长数组的转换案例====================")
        val fixedLenArr = Array(1, 2, 3)
        val unfixedLenArr = ArrayBuffer(4, 5, 6)

        val fixed2Unfixed = fixedLenArr.toBuffer
        fixed2Unfixed.append(7)
        println(s"定长转变长 -> $fixed2Unfixed")

        println("================================")
        val unFixed2Fixed = unfixedLenArr.toArray

        println(s"变长转定长 -> $unFixed2Fixed")

    }

    /**
      * 变长数组测试
      * 特点：
      * 创建后可以动态的添加，删除元素
      */
    def unfixedLengthArray(): Unit = {
        println("=================变长数组ArrayBuffer案例====================")
        //变长数组创建
        val arrBuf01 = ArrayBuffer.newBuilder[Float].result()
        val arrBuf02 = new ArrayBuffer[String]()
        val arrBuf03 = ArrayBuffer(1, "arr", 3)
        //往变长数组添加，删除，修改元素
        arrBuf01 += 2.0F
        arrBuf01 += 3.0F
        arrBuf01 ++= Array(1.0F, 3.0F)
        println(s"add element: arrBuf01 -> ${arrBuf01}")
        arrBuf01 -= 2.0F
        println("===========================================")
        println(s"delete element: arrBuf01 -> ${arrBuf01}")
        println("===========================================")

        arrBuf02 += "a"
        arrBuf02.insert(1, "b", "c", "d")
        arrBuf02 ++= Array("e", "f")
        println(s"add element: arrBuf02 -> ${arrBuf02}")
        println("===========================================")
        arrBuf02(0) = "aaa"
        arrBuf02.update(2, "bbbb")
        println(s"update element：arrBuf02 -> ${arrBuf02}")
        println("===========================================")
        arrBuf02 -= "b"
        arrBuf02.remove(0)
        arrBuf02 --= Array("c", "g")
        println(s"delete element: arrBuf02 -> ${arrBuf02}")
        println("===========================================")
        arrBuf03.append("DDD", "MMM")
        arrBuf03.appendAll(Array("EEE", "FFF"))
        //变长数组遍历
        for (i <- arrBuf03) {
            println(i)
        }
        println(s"arr length -> ${arrBuf01.length}")
    }

    /**
      * 定长数组测试
      * 特点：
      * 不可变集合
      * 数组创建后不能添加或删除元素，只能修改元素的值
      */
    def fixedLengthArray(): Unit = {
        println("=================定长数组Array案例====================")
        //定长数组创建方式
        val arr01 = new Array[Int](4)

        val arr02: Array[Int] = Array(1, 2, 3)

        val arr03: Array[Any] = Array(1, "2", 3)

        println(arr02)
        //scala类型推断
        val arr04 = Array(1, 2, 3)

        val arr05 = Array(1, "2", 3)

        //定长数组元素赋值
        for (i <- 1 until 4 if i % 2 == 0) {
            arr01(i) = i * 2
        }

        //定长数组遍历
        for (i <- arr01) {
            println(i)
        }
    }
}
