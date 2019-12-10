package com.sitech.gmall.realtime.util

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.slf4j.LoggerFactory

object StreamingContextUtils {


    private val logger = LoggerFactory.getLogger(StreamingContextUtils.getClass)

    /**
      * 创建streamingcontext
      *
      * @param appName
      * @param master
      * @param chkpointDir
      * @param batchInterval
      * @return
      */
    def createStreamingContext(appName: String,
                               master: String,
                               chkpointDir: String,
                               batchInterval: Duration) = {
        val conf = new SparkConf()
            .setAppName(appName)
            .set("spark.streaming.backpressure.enabled", "true")
            .set("spark.streaming.backpressure.initialRate", "10240")
            .set("spark.streaming.kafka.maxRatePerPartition", "102400")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")

        if (StringUtils.isEmpty(master)) {
            conf.setMaster(master)
        } else {
            conf.setMaster("local[*]")
        }
        val ssc = new StreamingContext(conf, batchInterval)
        if (StringUtils.isEmpty(chkpointDir)) {
            ssc.checkpoint(chkpointDir)
            logger.info(s"设置StreamingContext的checkpoint目录为: $chkpointDir")
        }
        ssc
    }


    /**
      * 创建 sparkstreaming程序正在运行的标签文件
      *
      * @param runningFlag
      */
    def createRunningFlag(runningFlag: File) = {
        if (!runningFlag.exists()) {
            runningFlag.createNewFile()
        }
    }


    /**
      * 启动streaming应用
      *
      * @param ssc
      * @param runningFlag
      */
    def startStreamingApplication(ssc: StreamingContext, runningFlag: File) = {
        if (runningFlag != null) {
            createRunningFlag(runningFlag)
        }
        ssc.start()
        ssc.awaitTermination()
    }

    /**
      * 实现sparkstreaming程序优雅的停止
      *
      * @param ssc         StreamingContext
      * @param runningFlag StreamingContext运行的标签文件
      */
    def stopGracefully(ssc: StreamingContext, runningFlag: File): Unit = {
        if (runningFlag != null) {
            val pool = Executors.newSingleThreadScheduledExecutor()
            //每隔5s检查sparkstreaming运行时标签是否存在，
            // 如不存在，优雅停止sparkstreaming
            pool.scheduleAtFixedRate(new Runnable {
                override def run(): Unit = {
                    if (!runningFlag.exists()) {
                        ssc.stop(true, true)
                        pool.shutdown()
                        System.exit(0)
                    }
                }
            }, 10L, 5, TimeUnit.SECONDS)
        }
    }
}
