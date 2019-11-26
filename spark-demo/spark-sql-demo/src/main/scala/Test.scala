import org.apache.spark.sql.SparkSession

object Test {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("dd")
            .enableHiveSupport()
            .getOrCreate()


        spark.udf.register("index", (arr: Seq[String], idx: Int) => arr(idx))
        val sql1 =
            """
              |--获得每个区域中城市商品点击次数top2
              |select t7.area,
              |    t7.product_name,
              |    t7.click_count,
              |    concat(
              |        --top1城市百分比
              |        concat(
              |            split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 1),'-')[1],
              |            ':',
              |            round(split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 1),'-')[0]/t7.click_count * 100,1),
              |            '%'
              |        ),
              |        ',',
              |        --top2城市百分比
              |        concat(
              |            split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 2),'-')[1],
              |            ':',
              |            round(split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 2),'-')[0]/t7.click_count * 100,1),
              |            '%'
              |        ),
              |        ',',
              |        concat(
              |            '其他',
              |            ':',
              |            round((t7.click_count - split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 2),'-')[0] - split(index(t7.sorted_city_click_count,size(t7.sorted_city_click_count) - 1),'-')[0])/t7.click_count * 100,1),
              |            '%'
              |        )
              |    ) as city_remark
              |from (
              |    --取出每个区域产品的点击次数top3
              |    select t6.area,
              |        t6.product_name,
              |        t6.click_count,
              |        sort_array(t6.city_click_count) as sorted_city_click_count,
              |        t6.rank_no
              |    from (
              |       --按区域分组，把每个区域中各个商品的点击次数降序排列，获得每个区域产品点击次数排名
              |        select t5.area,
              |            t5.product_name,
              |            t5.click_count,
              |            t5.city_click_count,
              |            row_number() over(partition by t5.area order by t5.click_count desc) as rank_no
              |        from (
              |            --计算每个产品在每个区域的点击次数，并聚合每个区域的每个城市对应的点击次数
              |            select t4.area,
              |                t4.product_name,
              |                sum(t4.click_count) as click_count,
              |                collect_list(concat(t4.click_count,'-',t4.city_name)) as city_click_count
              |            from (
              |                --计算每个产品在每个区域的每个城市的点击次数
              |                select t1.area,
              |                    t1.city_name,
              |                    t3.product_name,
              |                    count(t2.click_product_id) as click_count
              |                from rwd_dev.city_info t1 join (
              |                    select t.click_product_id,
              |                        t.city_id
              |                    from rwd_dev.user_visit_action t
              |                    where t.click_product_id != -1
              |                ) t2 on t2.city_id = t1.city_id
              |                join rwd_dev.product_info t3 on t3.product_id = t2.click_product_id
              |                group by t1.area,t1.city_name,t3.product_name
              |            ) t4
              |            group by t4.area,t4.product_name
              |        ) t5
              |    ) t6
              |    where t6.rank_no <= 3
              |) t7
            """.stripMargin

        spark.sql(sql1).show(100, false)

        spark.stop()
    }
}
