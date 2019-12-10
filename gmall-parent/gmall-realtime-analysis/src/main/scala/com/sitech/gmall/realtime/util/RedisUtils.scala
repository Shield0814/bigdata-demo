package com.sitech.gmall.realtime.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool

object RedisUtils {

    private val logger = LoggerFactory.getLogger(RedisUtils.getClass)

    @volatile private var pool: JedisPool = _

    /**
      * 构造单例的redis连接池
      *
      * @param host
      * @param port
      * @return
      */
    def getPool(host: String, port: Int) = {
        if (pool == null) {
            synchronized {
                if (pool == null) {
                    //redis对象池配置
                    val config = new GenericObjectPoolConfig
                    config.setMaxIdle(1)
                    config.setMaxTotal(5)
                    config.setFairness(true)
                    config.setTestOnBorrow(true)
                    config.setBlockWhenExhausted(true)
                    config.setMaxWaitMillis(10000)
                    pool = new JedisPool(config, host, port)
                    logger.info(s"创建jedis连接池成功:${pool}")
                }
            }
        }
        pool
    }

    /**
      * 从连接池获取redis连接
      *
      * @return
      */
    def getJedis = pool.getResource


}
