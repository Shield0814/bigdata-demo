package com.ljl.flink.stream.connector

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.client.Requests
import org.json4s.native.Serialization

case class SensorTemper(id: String, temper: Float)

object ElasticSearchConnectorSinkApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment


        val inputDataStream = env.fromCollection(Array(
            "sensor_0,12.1",
            "sensor_1,13.4",
            "sensor_2,15.4",
            "sensor_3,12.7",
            "sensor_0,13.2"
        ))
        val sensorTemperDataStream = inputDataStream.map {
            reading =>
                val fields = reading.split(",")
                SensorTemper(fields(0), fields(1).toFloat)
        }

        val config = new java.util.HashMap[String, String]
        config.put("cluster.name", "my-application")
        config.put("bulk.flush.max.actions", "1")

        val transportAddresses = new java.util.ArrayList[InetSocketAddress]
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))


        sensorTemperDataStream.addSink(new ElasticsearchSink(config,
            transportAddresses,
            new ElasticsearchSinkFunction[SensorTemper] {
                override def process(t: SensorTemper,
                                     ctx: RuntimeContext,
                                     indexer: RequestIndexer): Unit = {
                    implicit val formats = org.json4s.DefaultFormats

                    val indexReq = Requests.indexRequest().index("sensor")
                        .`type`("sensor_temp")
                        .id(t.id)
                        .source(Serialization.write(t))
                    indexer.add(indexReq)
                }
            }))


        env.execute("ElasticSearchConnectorSinkApp")
    }
}
