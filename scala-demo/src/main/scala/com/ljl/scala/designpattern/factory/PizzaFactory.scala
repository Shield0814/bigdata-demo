package com.ljl.scala.designpattern.factory

class PizzaFactory {

    def orderPizza(pizzaType: String): Pizza = {
        pizzaType match {
            case "chesses" => new ChessesPizza
            case "greek" => new GreekPizza
        }
    }

}
