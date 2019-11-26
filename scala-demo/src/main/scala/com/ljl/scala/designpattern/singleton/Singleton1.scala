package com.ljl.scala.designpattern.singleton


/**
  * 单类模式：饿汉式
  * 线程不安全,不是懒加载
  */
class Singleton1 private() {}

object Singleton1 {
    private val instance = new Singleton1

    def getInstance(): Singleton1 = {
        instance
    }
}
