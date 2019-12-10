package com.ljl.flink.sensor.sink

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.scala.typeutils.UnitSerializer
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

class TransactionalFileSink(val targetPath: String,
                            val tempPath: String)
    extends TwoPhaseCommitSinkFunction[SensorReading, String, Unit](new StringSerializer, new UnitSerializer) {

    override def invoke(txnId: String, value: SensorReading, ctx: SinkFunction.Context[_]): Unit = {

    }

    override def beginTransaction(): String = {

        ""
    }

    override def preCommit(transaction: String): Unit = {}


    override def commit(transaction: String): Unit = {

    }

    override def abort(transaction: String): Unit = {

    }
}
