package com.ljl.spark.streaming

import java.sql.DriverManager

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 工作类
  *
  * @param config 数据库jdbc连接配置参数
  */
class DBProxy(config: Map[String, String]) {

    private val logger: Logger = LoggerFactory.getLogger(classOf[DBProxy])
    Class.forName(config("driver"))
    private val connection = DriverManager.getConnection(config("url"), config("user"), config("password"))

    private val batchSize = 10


    /**
      * 批量新增数据
      *
      * @param sql    新增数据的sql
      * @param params sql的参数列表
      */
    def insertBatch(sql: String, params: Array[Array[Any]]) = {
        try {
            connection.setAutoCommit(false)
            val pstat = connection.prepareStatement(sql)
            for (i <- 1 to params.length) {
                //设置参数
                for (j <- 1 to params(i).length) {
                    pstat.setObject(i, params(i - 1))
                }
                //添加到批次
                pstat.addBatch()
            }
            //提交未提交的批次
            pstat.executeUpdate()
            connection.commit()
            if (logger.isInfoEnabled) {
                logger.info(s"数据插入成功,共插入${params.length}条")
            }
            true
        } catch {
            case e: Exception =>
                connection.rollback()
                logger.error("数据插入失败", e)
        }
        false

    }

    /**
      * 关闭数据库连接
      */
    def close = connection.close()
}

/**
  * 工作类工厂
  *
  * @param config
  */
class PooledDBProxyFactory(config: Map[String, String]) extends BasePooledObjectFactory[DBProxy] {
    override def create(): DBProxy = new DBProxy(config)

    override def wrap(t: DBProxy): PooledObject[DBProxy] = new DefaultPooledObject[DBProxy](t)

    override def destroyObject(p: PooledObject[DBProxy]): Unit = p.getObject.close
}

/**
  * 对象池
  */
object DBProxyPool {

    @volatile private var instance: GenericObjectPool[DBProxy] = null

    def getInstance(config: Map[String, String]): GenericObjectPool[DBProxy] = {
        if (instance == null) {
            synchronized {
                if (instance == null) {
                    val factory = new PooledDBProxyFactory(config)
                    val poolConfig = new GenericObjectPoolConfig
                    poolConfig.setMaxTotal(5)
                    poolConfig.setMaxIdle(1)
                    instance = new GenericObjectPool[DBProxy](factory, poolConfig)
                }
            }
        }
        instance
    }

}
