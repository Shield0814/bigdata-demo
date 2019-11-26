package com.ljl.scala.exercise.akka.spark.client

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.ljl.scala.exercise.akka.spark.common.{HeartBeat, RegisterWorkerInfo, RegisteredWorkerInfo, SendHeartBeat}
import com.typesafe.config.ConfigFactory

import scala.util.Random

class SparkWorker(masterHost: String, masterPort: Int, masterName: String) extends Actor {

    var masterRef: ActorSelection = _

    val workerId = "worker" + Random.nextInt(10)

    override def preStart(): Unit = {
        val path = s"akka.tcp://SparkMaster@${masterHost}:${masterPort}/user/${masterName}"
        masterRef = context.actorSelection(path)
    }

    override def receive: Receive = {
        case "start" =>
            masterRef ! RegisterWorkerInfo(workerId, 2, 1024 * 1024)
            println(s"worker 启动完成，master info $masterRef")
        case RegisteredWorkerInfo =>
            println(s"worker $workerId 注册成功")
            import context.dispatcher

            import scala.concurrent.duration._
            context.system.scheduler.schedule(1 second, 3 second, self, SendHeartBeat)
        case SendHeartBeat =>
            masterRef.!(HeartBeat(workerId))
    }
}

object SparkWorker {

    def main(args: Array[String]): Unit = {
        val (workerHost, workerPort) = ("127.0.0.1", 8088)
        val (masterHost, masterPort, masterName) = ("127.0.0.1", 7077, "SparkMasterActor01")

        val config = ConfigFactory.parseString(
            s"""
               |akka.actor.provider="akka.remote.RemoteActorRefProvider"
               |akka.remote.netty.tcp.hostname=$workerHost
               |akka.remote.netty.tcp.port=$workerPort
         """.stripMargin
        )
        val actorSystem = ActorSystem("SparkWorker", config)

        val workerRef = actorSystem.actorOf(Props(new SparkWorker(masterHost, masterPort, masterName)), "SparkWorkerActor01")

        workerRef.!("start")


    }
}

