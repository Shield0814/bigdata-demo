package com.ljl.scala.designpattern.singleton


/**
  * 单类模式：静态内部类
  */
class Singleton3 private() {

}

object Singleton3 {

    private[this] object Instance {
        val instance = new Singleton3
    }

    def getInstance(): Singleton3 = {
        Instance.instance
    }

}
