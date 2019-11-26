package com.ljl.spark.core

import java.util

import it.unimi.dsi.fastutil.ints._
import org.apache.spark.util.SizeEstimator

import scala.util.Random

object MemoryConsumptionEstimateTest {


    def main(args: Array[String]): Unit = {

        val random = Random
        val data1 = new util.HashMap[Int, Int]()

        val data2 = collection.mutable.Map[Int, Int]()

        val data3 = new Int2IntArrayMap()

        val data4 = new Int2IntAVLTreeMap()

        val data5 = new Int2IntRBTreeMap()
        //
        for (i <- 1 to 512000) {
            data1.put(random.nextInt(10000000), random.nextInt(1000000000))
            data2.put(random.nextInt(10000000), random.nextInt(1000000000))
            data3.put(random.nextInt(10000000), random.nextInt(1000000000))
            data4.put(random.nextInt(10000000), random.nextInt(1000000000))
            data5.put(random.nextInt(10000000), random.nextInt(1000000000))
        }


        val size1 = SizeEstimator.estimate(data1)
        val size2 = SizeEstimator.estimate(data2)
        val size3 = SizeEstimator.estimate(data3)
        val size4 = SizeEstimator.estimate(data4)
        val size5 = SizeEstimator.estimate(data5)


        println(size1)
        println(size2)
        println(size3)
        println(size4)
        println(size5)

        println("=" * 50)

        var start = System.currentTimeMillis()
        println(data1.get(random.nextInt(10000000)))
        var end = System.currentTimeMillis()
        println(end - start)


        start = System.currentTimeMillis()
        println(data2.get(random.nextInt(10000000)))
        end = System.currentTimeMillis()
        println(end - start)

        start = System.currentTimeMillis()
        println(data3.get(random.nextInt(10000000)))
        end = System.currentTimeMillis()
        println(end - start)

        start = System.currentTimeMillis()
        println(data4.get(random.nextInt(10000000)))
        end = System.currentTimeMillis()
        println(end - start)

        start = System.currentTimeMillis()
        println(data5.get(random.nextInt(10000000)))
        end = System.currentTimeMillis()
        println(end - start)


    }
}
