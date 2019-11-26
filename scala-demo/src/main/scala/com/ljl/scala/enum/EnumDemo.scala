package com.ljl.scala.enum

import com.ljl.scala.enum.SignalLight.SignalLight

object EnumDemo {

    def main(args: Array[String]): Unit = {

        val greenlight: SignalLight = SignalLight.GREEN_LIGHT
        println(greenlight)

    }
}

object SignalLight extends Enumeration {
    type SignalLight = Value
    val RED_LIGHT = Value("RED")
    val GREEN_LIGHT = Value("GREEN")
    val YELLOW_LIGHT = Value("YELLOW")
}
