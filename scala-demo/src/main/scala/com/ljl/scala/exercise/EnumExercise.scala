package com.ljl.scala.exercise

object EnumExercise extends App {


    for (elem <- 1 to 100) {
        new Thread() {
            override def run(): Unit = println(Suits.Club == Suits.Club)
        }.start()

    }
}

object Suits extends Enumeration {
    type Suits = Value
    val Spade = Value("♠")
    val Club = Value("♣")
    val Heart = Value("♥")
    val Diamond = Value("♦")

    override def toString(): String = {
        Suits.values.mkString(",")
    }

    def isRed(card: Suits) = card == Heart || card == Diamond
}
