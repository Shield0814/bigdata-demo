package com.ljl.scala.viewBound

object ViewBoundDemo {

    def main(args: Array[String]): Unit = {

        def greaterOne[T <% Animal](seq: Seq[T]): T = {
            var max = 0
            for (i <- 1 until seq.length) {
                if (seq(i).compare(seq(max)) > 0) max = i
            }
            seq(max)
        }

        val cat1 = new Cat

        val whiteCat = new WhiteCat("white")
        whiteCat.age = 10

        //大招时间:Int类型是不能作为 greateOne函数的入参的，但是定义一个Int到Animal/Cat/WhiteCat的隐式转换就可以了
        //所以视图界定是可以接受隐式转换的类型，但是上界不行
        //没有隐式转换的话会报以下错误：
        // Error:(21, 27) No implicit view available from Any => com.ljl.scala.viewBound.Animal.
        //        println(greaterOne[Animal](Array(whiteCat,cat1, new Animal("animal", 3),3)))
        implicit def int2Animal(p: Int): Animal = new Animal("animal", p)

        val animals = Array[Animal](whiteCat, cat1, new Animal("animal", 3), 30)
        println(greaterOne(animals))
    }


}

class Animal(var name: String, var age: Int) extends Ordered[Animal] {


    def this() {
        this(null, -99)
    }

    override def compare(that: Animal): Int = this.age - that.age


    override def toString = s"Animal($name, $age)"
}

class Cat extends Animal("cat", 1) {
    override def toString: String = s"Cat($name,$age)"
}


class WhiteCat(var color: String) extends Cat {
    override def toString: String = s"WhiteCat($name,$age,$color)"
}
