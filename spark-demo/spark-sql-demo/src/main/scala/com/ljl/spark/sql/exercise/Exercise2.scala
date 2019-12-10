package com.ljl.spark.sql.exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class TUserEvent(user_id: String, event_ts: Long)

object Exercise2 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Exercise2").setMaster("local[1]")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._

        spark.sparkContext.parallelize(Array(
            "A	1566300034",
            "A	1566300044",
            "A	1566300050",
            "A	1566300150",
            "A	1566300180",
            "A	1566300300",
            "B	1566300050",
            "B	1566300060",
            "B	1566300110",
            "B	1566300210",
            "B	1566300270",
            "B	1566300295"
        ), 1).map(line => {
            val s = line.split("\t")
            TUserEvent(s(0), s(1).toLong)
        }).toDS().createOrReplaceTempView("t_user_event")

        val sql =
            """
              |select t3.user_id,
              |    t3.event_ts,
              |    t3.diff_ts,
              |    t3.tmp_group_id + 1 as group_id
              |from (
              |    select t2.user_id,
              |        t2.event_ts,
              |        t2.diff_ts,
              |        t2.boundary,
              |        sum(t2.boundary) over(partition by t2.user_id order by t2.event_ts)  as tmp_group_id
              |    from (
              |        select t1.user_id,
              |            t1.event_ts,
              |            t1.lag_event_ts,
              |            t1.event_ts - t1.lag_event_ts  as diff_ts,
              |            case when (t1.event_ts - t1.lag_event_ts) > 50 then 1 else 0 end as boundary
              |        from (
              |            select t.user_id,
              |                t.event_ts,
              |                lag(t.event_ts,1,t.event_ts) over(partition by t.user_id order by t.event_ts) as lag_event_ts
              |            from t_user_event t
              |            order by t.user_id
              |        ) t1
              |    ) t2
              |) t3
            """.stripMargin

        spark.sql(sql).show(100, false)
        spark.stop()


    }
}
