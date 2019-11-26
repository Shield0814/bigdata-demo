package com.ljl.flink.stream.checkpoint

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckpointApp {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //内存后端
        val memorySB = new MemoryStateBackend(5 * 1024 * 1024, true)

        //文件后端
        val checkpointDataUri = "hdfs://bigdata116:8020/user/sysadm/flink/checkpoints"
        val fsSB = new FsStateBackend(checkpointDataUri, true)

        //rockdb后端
        val rockDbSB = new RocksDBStateBackend(checkpointDataUri, true)

        env.setStateBackend(memorySB)

        //是否在job取消后保存checkpoint 状态
        //RETAIN_ON_CANCELLATION： 如果任务取消，将保留checkpoint状态，任务取消后必须手动删除checkpoint状态
        //DELETE_ON_CANCELLATION: checkpoint状态只在任务失败时可用
        val config: CheckpointConfig = env.getCheckpointConfig
        config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
