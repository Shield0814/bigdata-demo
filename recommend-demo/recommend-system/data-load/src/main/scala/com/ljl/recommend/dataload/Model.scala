package com.ljl.recommend.dataload


/**
  * 电影信息
  *
  * @param mid       电影id
  * @param name      电影名称
  * @param descri    电影描述
  * @param timelong  时长
  * @param issue     发布时间
  * @param shoot     拍摄时间
  * @param language  语言
  * @param genres    类型
  * @param actors    演员
  * @param directors 导演
  */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)

/**
  * 评分信息
  *
  * @param userId    用户id
  * @param movieId   电影id
  * @param rate      用户给电影评分
  * @param timestamp 评分时间
  */
case class Rating(userId: Int, movieId: Int, rate: Double, timestamp: Long)

/**
  * 标签信息
  *
  * @param userId    用户id
  * @param movieId   电影id
  * @param tag       用户给电影打的标签
  * @param timestamp 打标签时间
  */
case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long)

/**
  * mongodb 配置信息
  *
  * @param uri 连接url
  * @param db  数据库名称
  */
case class MongoConfig(uri: String, db: String)

/**
  * es 配置信息
  *
  * @param httpHosts
  * @param transportHosts
  * @param index       索引名称
  * @param clustername 集群名称
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)
