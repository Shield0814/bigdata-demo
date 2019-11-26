package com.ljl.scala.exercise

import scala.beans.BeanProperty

object ClassExercise {

    def main(args: Array[String]): Unit = {

        val tom = new Student(1, "tom")
    }

}

//编写一个Time类，加入只读属性hours和minutes，
// 和一个检查某一时刻是否早于另一时刻的方法before(other:Time):Boolean。
// Time对象应该以new Time(hrs,min)方式构建。
class Time(val hours: Int, val minutes: Int) {

    def before(other: Time): Boolean = {
        hours * 60 + minutes < other.hours * 60 + other.minutes
    }
}

//创建一个Student类，加入可读写的JavaBeans属性name(类型为String)和id(类型为Long)。
// 有哪些方法被生产？
//1.name_eq(String name),name()
//2.id_eq(Long id),id()
//5.setName(String name) getName
//6.setId(Long id) getId()
// (用javap查看。)你可以在Scala中调用JavaBeans的getter和setter方法吗？
class Student(@BeanProperty var id: Long, @BeanProperty var name: String)