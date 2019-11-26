package com.ljl.scala.collection

import java.util.concurrent.ConcurrentHashMap

object MapDemo extends App {

    //    testMutableMap()
    testMapClone()

    def testMapClone(): Unit = {
        val map = collection.mutable.Map[String, Int]()
        map += ("hello" -> 2)

        val map1 = map.clone()
        map1 += ("hello" -> 3)

        println(map)
        println(map1)
    }

    //测试不可变map,不可变map修改后都产生新的map，原map不变
    def testImmutableMap(): Unit = {
        val immutableMap = Map[String, Int](("a", 1))
        println(immutableMap)

        //return A ***new*** immutable map containing updating this map with a given key/value mapping.
        val updated = immutableMap.updated("a", 2)
        println(updated)


        val immutableMap2 = updated.+(("b", 1))
        println(immutableMap2)

        println("*" * 20)
        val immutableMap3 = immutableMap + ("c" -> 1)
        println(immutableMap3)

        println("*" * 20)
        val immutableMap5 = immutableMap2 + (("a", 10), ("bc", 11), ("dd", 12))
        println(immutableMap5)

        println("*" * 20)
        val immutableMap4 = immutableMap2 ++ immutableMap3
        println(immutableMap4)


    }

    def testMutableMap(): Unit = {
        val map1 = new ConcurrentHashMap[String, Int]()

        val lock = "dd"
        //        val map1: util.Map[String, Integer] = Collections.synchronizedMap(new util.HashMap[String, Integer]())

        for (elem <- 1 to 100) {
            new Thread(new Runnable {
                override def run(): Unit = {
                    map1.put("hello", map1.getOrDefault("hello", 0) + 1)
                }
            }, String.valueOf(elem)).start()
        }
        Thread.sleep(1000)
        println(map1)

    }
}
