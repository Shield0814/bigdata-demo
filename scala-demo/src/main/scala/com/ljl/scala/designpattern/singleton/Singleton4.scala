package com.ljl.scala.designpattern.singleton

/**
  * 单类模式：DLC
  * 线程安全，懒加载
  */
class Singleton4 private {

}

object Singleton4 {

    @volatile private var instance: Singleton4 = _

    def getInstance(): Singleton4 = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    instance = new Singleton4
                }
            }
        }
        instance

    }
}
