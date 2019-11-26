package com.ljl.scala.exercise.akka.yellowchicken.client

import akka.actor.{Actor, ActorPath, ActorRef, ActorSelection, ActorSystem, Props}
import com.ljl.scala.exercise.akka.yellowchicken.common.{ClientMessage, ServerMessage}
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

/**
  * 基于akka实现的简单的交互功能
  * client 端：向服务端发送消息，并接受服务端的回复
  *
  * @param serverHost 服务端的ip地址
  * @param serverPort 服务端的端口
  * @param serverName 服务端服务名称
  */
class YellowChickenClient(var serverHost: String,
                          var serverPort: Int,
                          serverName: String) extends Actor {

    var serverRef: ActorSelection = _


    override def preStart(): Unit = {
        val serverUrl = s"akka.tcp://server@$serverHost:$serverPort/user/$serverName"
        val ap = ActorPath.fromString(serverUrl)
        serverRef = context.actorSelection(ap)
        println(serverRef)
    }

    override def receive: Receive = {
        case "start" => println("start,小黄鸡客户端启动，可以交流了")
        case "exit" =>
            context.stop(self)
            context.system.shutdown()
            println("exit,小黄鸡客户端退出")
        case msg: String =>
            serverRef ! ClientMessage(msg)
        case ServerMessage(msg) =>
            println("服务端：" + msg)
    }
}

object YellowChickenClient extends App {

    val (clientHost, clientPort, serverHost, serverPort, serverName) = ("127.0.0.1", 9990, "127.0.0.1", 9999, "YellowChickenServer")

    private val config = ConfigFactory.parseString(
        s"""
           |akka.actor.provider="akka.remote.RemoteActorRefProvider"
           |akka.remote.netty.tcp.hostname=$clientHost
           |akka.remote.netty.tcp.port=$clientPort
         """.stripMargin
    )

    private val clientActorSystem = ActorSystem("client", config)


    private val clientRef: ActorRef = clientActorSystem.actorOf(
        Props(new YellowChickenClient(serverHost, serverPort, serverName)),
        "YellowChickenClient"
    )

    clientRef ! "start"

    var isRunning = true
    var question: String = _
    while (isRunning) {
        question = StdIn.readLine("请输入您要咨询的问题：")
        if ("exit".equals(question)) {
            isRunning = false
        }
        clientRef ! question
    }


}
