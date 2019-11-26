package com.ljl.scala.designpattern.singleton

object SingletonDemo {

    def main(args: Array[String]): Unit = {
        val o2 = Singleton1.getInstance()

        for (i <- 1 to 100000) {
            new Thread {
                val o1 = Singleton1.getInstance()
                if (o2 != o1) {
                    println(s"$o2,$o1")
                }
            }.start()
        }

    }
}
