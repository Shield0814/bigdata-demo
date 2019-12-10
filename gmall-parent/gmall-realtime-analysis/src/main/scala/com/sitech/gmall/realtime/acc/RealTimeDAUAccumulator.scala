package com.sitech.gmall.realtime.acc

import com.sitech.gmall.common.GmallConstants
import com.sitech.gmall.config.GmallConfigs
import com.sitech.gmall.realtime.util.RedisUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.AccumulatorV2
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable


class RealTimeDAUAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    var dauAcc: mutable.Map[String, Long] = mutable.Map.empty[String, Long]

    override def isZero: Boolean = dauAcc.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
        val newDauAcc = new RealTimeDAUAccumulator
        for ((hour, dau) <- this.dauAcc) {
            newDauAcc.dauAcc.put(hour, dau)
        }
        newDauAcc
    }

    override def reset(): Unit = dauAcc.clear()

    override def add(v: String): Unit = {
        val hourDau = this.dauAcc.getOrElseUpdate(v, 0L) + 1L
        this.dauAcc.put(v, hourDau)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
        val oAcc = other.asInstanceOf[RealTimeDAUAccumulator]
        for ((hour, dau) <- oAcc.dauAcc) {
            this.dauAcc.put(hour, this.dauAcc.getOrElseUpdate(hour, 0L) + dau)
        }
    }

    override def value: mutable.Map[String, Long] = this.dauAcc
}


/**
  * 实时日活累加器单例对象
  */
object RealTimeDAUAccumulator {

    private val logger: Logger = LoggerFactory.getLogger(RealTimeDAUAccumulator.getClass)
    @volatile var dauAccumulator: RealTimeDAUAccumulator = _

    def getDauAccumulator(ssc: StreamingContext) = {
        if (dauAccumulator == null) {
            synchronized {
                if (dauAccumulator == null) {
                    dauAccumulator = new RealTimeDAUAccumulator
                    ssc.sparkContext.register(dauAccumulator, "dauAccumulator")
                    val pool = RedisUtils.getPool(GmallConfigs.REDIS_HOST, GmallConfigs.REDIS_PORT)
                    val jedis = pool.getResource
                    val oldDau = jedis.hgetAll(GmallConstants.REDIS_DAU_HOUR_KEY)
                    val oldAcc = mutable.Map.empty[String, Long]
                    oldDau.foreach(kv => oldAcc.put(kv._1, kv._2.toLong))
                    dauAccumulator.dauAcc = oldAcc
                    jedis.close()
                }
            }
        }
        dauAccumulator
    }
}


