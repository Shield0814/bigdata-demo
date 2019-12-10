package com.ljl.spark.sql.exercise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class TOrder(oid: Int, uid: Int, otime: String, oamount: Double)

object Exercise1 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Exercise1").setMaster("local[1]")
        val spark = SparkSession.builder().config(conf).getOrCreate()

        import spark.implicits._
        val tOrderDS = spark.sparkContext.parallelize(Array(
            "1003,2,2018-01-01,100",
            "1004,2,2018-01-02,20",
            "1005,2,2018-01-02,100",
            "1006,4,2018-01-02,30",
            "1007,1,2018-01-03,130",
            "1008,2,2018-01-03,5",
            "1009,2,2018-01-03,5",
            "1001,5,2018-02-01,110",
            "1002,3,2018-02-01,110",
            "1003,3,2018-02-03,100",
            "1004,3,2018-02-03,20",
            "1005,3,2018-02-04,30",
            "1006,6,2018-02-04,100",
            "1007,6,2018-02-04,130",
            "1001,1,2018-03-01,120",
            "1002,2,2018-03-03,5",
            "1003,2,2018-03-03,11",
            //            "1006,4,2018-03-04,30",
            "1007,1,2018-03-04,50",
            "1004,3,2018-03-03,1",
            "1005,3,2018-03-04,20"
        ), 1).map(line => {
            val s = line.split(",")
            TOrder(s(0).toInt, s(1).toInt, s(2), s(3).toDouble)
        }).toDS()
        tOrderDS.createOrReplaceTempView("t_order")
        spark.udf.register("index", (arr: Seq[String], idx: Int) => arr(idx))


        val sql =
            """
              |select t5.uid,
              |    t5.ocount as big_order_count,
              |    case when t5.osize = 0 then 0
              |        else split(index(t5.sortedOamount,0),'&')[1]
              |    end as first_order_amount,
              |    case when t5.osize = 0 then 0
              |        else split(index(t5.sortedOamount,t5.osize - 1),'&')[1]
              |    end as last_order_amount
              |from (
              |    select t4.uid,
              |        t4.ocount,
              |        size(t4.otime_oamount) as osize,
              |        sort_array(t4.otime_oamount) as sortedOamount
              |    from (
              |        select t2.uid,
              |            sum(case when t3.uid is not null and t3.oamount > 10 then 1 else 0 end) as ocount,
              |            collect_set(concat(t3.otime,'&',t3.oamount)) as otime_oamount
              |        from (
              |            --2018年1月份下过订单, 2018年2月份没下过订单的用户
              |            select t1.uid as uid
              |            from (
              |                select  t.uid,
              |                        min(date_format(t.otime,'yyyyMM')) over(partition by t.uid) as min_omonth,
              |                        max(date_format(t.otime,'yyyyMM'))  over(partition by t.uid) as max_omonth
              |                from t_order t
              |                where date_format(t.otime,'yyyyMM') >= '201801'
              |                    and date_format(t.otime,'yyyyMM') <= '201802'
              |            ) t1
              |            where t1.min_omonth = '201801'
              |                and t1.max_omonth = '201801'
              |            group by t1.uid
              |        ) t2 left join (
              |            --2018年3月的订单数据
              |            select t.oid,
              |                t.uid,
              |                t.otime,
              |                t.oamount
              |            from t_order t
              |            where date_format(t.otime,'yyyyMM') = '201803'
              |        ) t3 on t3.uid = t2.uid
              |        group by t2.uid
              |    ) t4
              |) t5
            """.stripMargin

        spark.sql(sql).show(100, false)
        spark.stop()
    }

}
