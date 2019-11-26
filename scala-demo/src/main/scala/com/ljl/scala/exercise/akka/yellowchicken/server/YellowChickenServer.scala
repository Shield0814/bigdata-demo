package com.ljl.scala.exercise.akka.yellowchicken.server

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.ljl.scala.exercise.akka.yellowchicken.common.{ClientMessage, ServerMessage}
import com.typesafe.config.ConfigFactory

/**
  * 基于akka实现的简单的交互功能
  * server端：接受客户端消息并回复客户端消息
  */
object YellowChickenServer extends App {


    val host = "127.0.0.1"
    val port = 9999


    private val config = ConfigFactory.parseString(
        s"""
           |akka.actor.provider="akka.remote.RemoteActorRefProvider"
           |akka.remote.netty.tcp.hostname=$host
           |akka.remote.netty.tcp.port=$port
         """.stripMargin
    )
    private val severActorSystem = ActorSystem("server", config)

    private val yellowChickenServer: ActorRef = severActorSystem.actorOf(Props[YellowChickenServer], "YellowChickenServer")


    yellowChickenServer.!("start")

    //    severActorSystem.shutdown()


    def apply(host: String, port: Int): YellowChickenServer = {

        null
    }


}

class YellowChickenServer(val host: String, val port: Int) extends Actor {

    def this() {
        this(null, 0)
    }

    override def receive: Receive = {
        case "start" => println("start,小黄鸡服务端启动")
        case ClientMessage(msg) =>
            msg match {
                case "大数据学费" => sender() ! ServerMessage("大数据学费20800RMB")
                case "学校地址" => sender() ! ServerMessage("北京")
                case "学习什么课程" => sender() ! ServerMessage("大数据 python scala JavaEE")
                case _ => sender() ! ServerMessage("小黄鸡刚刚神游太虚了，不知道您在说什么，能重新说一下吗?")
            }

    }
}
