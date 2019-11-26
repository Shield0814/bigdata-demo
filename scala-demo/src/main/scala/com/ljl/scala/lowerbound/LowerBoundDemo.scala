package com.ljl.scala.lowerbound

/**
  * scala中对于下界来说：
  * 对于下界，可以传入任意类型
  * 传入和Animal直系的，是Animal父类的还是父类处理，是Animal子类的按照Animal处理
  * 和Animal无关的，一律按照Object处理
  * 也就是下界，可以随便传，只是处理是方式不一样
  * 不能使用上界的思路来类推下界的含义
  */
object LowerBoundDemo extends App {

    def biophony[T >: Animal](things: Seq[T]) = things

    //Bird 继承 Animal,Animal 继承 Earth, Moon和Animal没有关系
    private val earths: Seq[Earth] = biophony(Seq(new Earth))
    private val animals1: Seq[Animal] = biophony(Seq(new Animal))
    private val animals2: Seq[Animal] = biophony(Seq(new Bird))

    private val objects: Seq[Object] = biophony(Seq(new Moon))
    private val str: String = 0.asInstanceOf[String]
    println(str)

    def reduceLeft[B >: Int](seq: Seq[Int])(op: (B, Int) => B): B = {
        if (seq.isEmpty)
            throw new UnsupportedOperationException("empty.reduceLeft")

        var first = true
        var acc: B = 0.asInstanceOf[B]
        0.asInstanceOf[Object]

        for (x <- seq) {
            if (first) {
                acc = x
                first = false
            }
            else acc = op(acc, x)
        }
        acc
    }


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