package com.ljl.scala.designpattern.factory

import scala.io.StdIn

object PizzaStore {

    def main(args: Array[String]): Unit = {
        var isRunning = true
        val factory = new PizzaFactory
        var pizzaType = StdIn.readLine("你想订购什么类型的pizza:\n")
        import scala.util.control.Breaks._
        breakable {
            while (isRunning) {
                if ("exit".equals(pizzaType)) {
                    isRunning = false
                    break()
                }
                val pizza = factory.orderPizza(pizzaType)
                pizzaType = StdIn.readLine("你想订购什么类型的pizza:\n")
            }
        }


    }
}
