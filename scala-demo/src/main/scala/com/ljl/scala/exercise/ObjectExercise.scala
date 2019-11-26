package com.ljl.scala.exercise

object ObjectExercise {

    def main(args: Array[String]): Unit = {
        Point(1, 2)
    }
}


//定义一个 Point 类和一个伴生对象,
// 使得我们可以不用 new 而直接用 Point(3,4)来构造 Point 实例 apply 方法的使用
class Point private(val x: Int, val y: Int)

object Point {
    def apply(x: Int, y: Int): Point = new Point(x, y)
}