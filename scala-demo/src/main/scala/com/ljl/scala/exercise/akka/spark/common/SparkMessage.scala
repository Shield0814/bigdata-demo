package com.ljl.scala.exercise.akka.spark.common


case class RegisterWorkerInfo(id: String, cpu: Int, ram: Long)

class WorkerInfo(val id: String, val cpu: Int, val ram: Long) {
    var lastHeartBeat = System.currentTimeMillis()
}

case object RegisteredWorkerInfo

case object SendHeartBeat

case class HeartBeat(id: String)

case object CheckTimeoutWorker

case object RemoveTimeoutWorker
