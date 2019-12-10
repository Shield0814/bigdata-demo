package com.ljl.flink.production

import java.time.Instant
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
  * 生产环境准备注意实现
  * 1. 显式指定算子的最大并行度
  * 128 : for all parallelism <= 128.
  * MIN(nextPowerOfTwo(parallelism + (parallelism / 2)), 2**15) : for all parallelism > 128.
  *
  * 2. 为每个算子设置UUID
  * 0 < parallelism <= max parallelism <= 2**15
  *
  * 3. 选择合适的状态后端
  * 推荐RockDBStateBackend,[MemoryStateBackend,FsStateBackend]
  *
  * 4. 保证JobManager的高可用
  **/
object ProductionReadinessChecklistApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //显式指定算子的最大并行度
        env.setMaxParallelism(100)

        //选择合适的状态后端
        val checkpointDataDir = "hdfs://bigdata116:8020/user/sysadm/flink-chk"
        val rocksBackend = new RocksDBStateBackend(checkpointDataDir, true)
        rocksBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)
        env.setStateBackend(rocksBackend)

        //设置checkpiont间隔
        //开启exactly-once checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE)
        //设置checkpoint失败后job不失败
        env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

        //为了避免checkpoint占用的时间过长，设置checkpoint超时时间,checkpoint必须在该时间范围内完成，否则将被取消，
        env.getCheckpointConfig.setCheckpointTimeout(300000)

        //make sure we process at least 30s without checkpointing
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
        //设置同时进行checkpoint的线程数
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(3)

        //如果用户取消任务的话删除checkpoint数据
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)


        //设置flink应用重启策略: noRestart, fixedDelayRestart,failureRateRestart
        env.getConfig.setRestartStrategy(RestartStrategies.noRestart())


        //kafka 配置参数
        val props: Properties = new Properties()
        props.setProperty("bootstrap.servers", "bigdata116:9092,bigdata117:9092,bigdata118:9092")
        props.setProperty("group.id", "flink-consumer-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")

        val sourceTopic = "test"
        val consumer = new FlinkKafkaConsumer011[String](sourceTopic, new SimpleStringSchema(), props)
        consumer.setStartFromGroupOffsets()

        val inputDataStream = env.addSource(consumer).uid("inputDataStream").name("inputDataStream")


        val sinkProps = new Properties()
        sinkProps.put("bootstrap.servers", "bigdata116:9092,bigdata117:9092,bigdata118:9092")
        sinkProps.put("acks", "-1")
        sinkProps.put("buffer.memory", "33554432")
        sinkProps.put("max.block.ms", "60000")
        sinkProps.put("batch.size", "16384")
        sinkProps.put("linger.ms", "5")
        sinkProps.put("transaction.timeout.ms", "60000") //必须设置
        sinkProps.put("retries", "1000000")

        val sinkTopic = "test-2pc"
        val kafkaSink = new FlinkKafkaProducer011[String](sinkTopic,
            new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
            sinkProps,
            Semantic.EXACTLY_ONCE)

        inputDataStream.map(s"${Instant.now()}:" + _)
            .addSink(kafkaSink).setParallelism(1).uid("kafkaSink").name("kafkaSink")

        env.execute("ProductionReadinessChecklistApp")

    }
}
