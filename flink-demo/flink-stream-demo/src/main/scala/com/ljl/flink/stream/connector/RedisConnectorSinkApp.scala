package com.ljl.flink.stream.connector

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisConnectorSinkApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val inputDataStream = env.fromCollection(Array(
            "sensor_0,12.1",
            "sensor_1,13.4",
            "sensor_2,15.4",
            "sensor_3,12.7",
            "sensor_0,13.2"
        )).setParallelism(1)

        val sensorTemperDataStream = inputDataStream.map {
            reading =>
                val fields = reading.split(",")
                SensorTemper(fields(0), fields(1).toFloat)
        }
        val redisHost = "bigdata116"

        val builder = new FlinkJedisPoolConfig.Builder
        builder.setDatabase(0)
            .setHost(redisHost)
            .setPort(6379)

        sensorTemperDataStream.addSink(new RedisSink[SensorTemper](builder.build(), new RedisMapper[SensorTemper] {
            override def getCommandDescription: RedisCommandDescription =
                new RedisCommandDescription(RedisCommand.HSET, "sensor_temper")

            override def getKeyFromData(data: SensorTemper): String = data.id

            override def getValueFromData(data: SensorTemper): String = data.temper.toString
        })).setParallelism(1)
        env.execute("RedisConnectorSinkApp")
    }
}
