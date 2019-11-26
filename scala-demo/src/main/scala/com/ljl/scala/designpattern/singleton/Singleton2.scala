package com.ljl.scala.designpattern.singleton

/**
  * 单例模式：懒汉式
  * 线程不安全，懒加载
  */
class Singleton2 private() {

}

object Singleton2 {

    var instance: Singleton2 = null;

    def getInstance(): Singleton2 = {
        if (instance == null) {
            instance = new Singleton2

        }
        instance
    }
}

