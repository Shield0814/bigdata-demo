package com.ljl.scala.designpattern.factory

class ChessesPizza extends Pizza {

    println("chessessPizza for you")

    override def prepare(): Unit = println("ChessesPizza:prepare()")

    override def bake(): Unit = println("ChessesPizza:bake()")

    override def cut(): Unit = println("ChessesPizza:cut()")

    override def box(): Unit = println("ChessesPizza:box()")
}
