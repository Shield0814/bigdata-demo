package com.ljl.scala.designpattern.factory

class GreekPizza extends Pizza {

    println("GreekPizza for you")

    override def prepare(): Unit = println("GreekPizza:prepare()")

    override def bake(): Unit = println("GreekPizza:bake()")

    override def cut(): Unit = println("GreekPizza:cut()")

    override def box(): Unit = println("GreekPizza:box()")
}
