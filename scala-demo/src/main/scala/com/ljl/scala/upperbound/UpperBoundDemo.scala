package com.ljl.scala.upperbound

object UpperBoundDemo extends App {


}

class Earth { //Earth 类
    def sound() { //方法
        println("hello !")
    }
}

class Animal extends Earth {
    override def sound() = { //重写了Earth的方法sound()
        println("animal sound")
    }
}

class Bird extends Animal {
    override def sound() = { //将Animal的方法重写
        print("bird sounds")
    }
}

class Moon {
    def sayHello() {
        println("hell0")
    }

}