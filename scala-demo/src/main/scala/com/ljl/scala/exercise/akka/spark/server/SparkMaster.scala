package com.ljl.scala.exercise.akka.spark.server

import akka.actor.{Actor, ActorSystem, Props}
import com.ljl.scala.exercise.akka.spark.common._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

class SparkMaster(val host: String, val port: Int) extends Actor {

    def this() {
        this("127.0.0.1", 7077)
    }

    val workerRefs = mutable.Map[String, WorkerInfo]()

    override def receive: Receive = {
        case "start" =>
            println(s"spark master 启动成功, master 信息如下：（$host,$port）...")
            self ! CheckTimeoutWorker
        case RegisterWorkerInfo(id, cpu, ram) =>
            if (!workerRefs.contains(id)) {
                workerRefs += (id -> new WorkerInfo(id, cpu, ram))
                sender() ! RegisteredWorkerInfo
            }
            println(s"已注册worker有${workerRefs.values}")
        case HeartBeat(id) =>
            workerRefs(id).lastHeartBeat = System.currentTimeMillis()
            println(s"master 端对worker $id 的心跳时间已更新")
        case CheckTimeoutWorker =>
            import context.dispatcher

            import scala.concurrent.duration._
            context.system.scheduler.schedule(1 second, 6 second, self, RemoveTimeoutWorker)
        case RemoveTimeoutWorker =>
            removeTimeoutWorker()
            println(s"当前存活的的worker：${workerRefs.values}")
    }

    /**
      * 移除超时的worker
      */
    def removeTimeoutWorker(): Unit = {
        val currentTime = System.currentTimeMillis()
        workerRefs.filter {
            case (id, workerInfo) =>
                if ((currentTime - workerInfo.lastHeartBeat) > 9000)
                    true
                else
                    false
        }.foreach(w => {
            workerRefs.remove(w._1)
            println(s"worker ${w._1} 已经停止工作")
        })

    }


}

object SparkMaster {

    def main(args: Array[String]): Unit = {
        val (host, port) = ("127.0.0.1", 7077)
        val config = ConfigFactory.parseString(
            s"""
               |akka.actor.provider="akka.remote.RemoteActorRefProvider"
               |akka.remote.netty.tcp.hostname=$host
               |akka.remote.netty.tcp.port=$port
         """.stripMargin
        )
        val actorSystem = ActorSystem("SparkMaster", config)

        val sparkMasterRef = actorSystem.actorOf(Props(new SparkMaster(host, port)), "SparkMasterActor01")

        sparkMasterRef ! "start"
        actorSystem.awaitTermination()

    }
}
