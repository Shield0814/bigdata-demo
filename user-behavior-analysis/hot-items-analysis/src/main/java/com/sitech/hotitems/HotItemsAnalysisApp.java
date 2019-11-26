package com.sitech.hotitems;

import com.sitech.hotitems.entity.CategoryItemStat;
import com.sitech.hotitems.entity.UserBehavior;
import com.sitech.hotitems.pf.CategoryItemPVAggregateFunction;
import com.sitech.hotitems.pf.TopNKeyedProcessFunction;
import com.sitech.hotitems.pf.WindowResultFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HotItemsAnalysisApp {

    private static Logger logger = LoggerFactory.getLogger(HotItemsAnalysisApp.class);

    public static void main(String[] args) {

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //使用event time进行分析
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            String filePath = "D:\\projects\\bigdata-demo\\user-behavior-analysis\\hot-items-analysis\\src\\main\\resources\\user-behaviors.csv";
            //获取用户行为数据，并分配时间戳和水位线
            SingleOutputStreamOperator<UserBehavior> userBehaviors = env.readTextFile(filePath).setParallelism(1)
                    .map(new MapFunction<String, UserBehavior>() {
                        //把一行数据映射成一个UserBehavior
                        @Override
                        public UserBehavior map(String line) throws Exception {
                            String[] splits = line.split(",");
                            UserBehavior ub = new UserBehavior();
                            ub.setUserId(Long.parseLong(splits[0]));
                            ub.setItemId(Long.parseLong(splits[1]));
                            ub.setCategoryId(Integer.parseInt(splits[2]));
                            ub.setBehavior(splits[3]);
                            //源数据时间戳单位为表，这里转成毫秒
                            ub.setTimestamp(Long.parseLong(splits[4]) * 1000);
                            return ub;
                        }
                        //分配时间戳和水位线，允许最大迟到5分钟
                    })
                    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {
                        @Override
                        public long extractTimestamp(UserBehavior ub) {
                            return ub.getTimestamp();
                        }
                    });


//            userBehaviors.print();
            statHotItemsTopN(userBehaviors);
//            categoryItemStat.print();


            env.execute("HotItemsAnalysisApp");
        } catch (Exception e) {
            logger.error("热门分析模块运行时发生异常!", e);
            System.exit(1);
        }
    }

    /**
     * 实时统计近1小时内各个品类的热门商品top10，每5分钟更新一次,热门度用浏览次数("pv")来衡量
     *
     * @param userBehaviors
     */
    private static DataStream<CategoryItemStat> statHotItemsTopN(SingleOutputStreamOperator<UserBehavior> userBehaviors) {

        //按商品品类和商品名称分类的用户浏览行为数据
        KeyedStream<UserBehavior, Tuple> pvubByCategoryAndItem = userBehaviors.filter(new FilterFunction<UserBehavior>() {
            //过滤出用户浏览行为数据
            @Override
            public boolean filter(UserBehavior ub) throws Exception {
                return "pv".equals(ub.getBehavior());
            }
        }).keyBy("categoryId", "itemId"); //指定keyBy时最好用字端设置


        //每个窗口中每个品类各个商品的浏览次数
        SingleOutputStreamOperator<CategoryItemStat> categoryItemCnt = pvubByCategoryAndItem.timeWindow(Time.hours(1), Time.minutes(5))
                //一定要使用匿名内部类的方式创建侧输出标签OutputTag
                .sideOutputLateData(new OutputTag<UserBehavior>("lateness-behavior-pv") {
                })
                //按商品品类和商品聚合每个品类商品的浏览次数
                .aggregate(new CategoryItemPVAggregateFunction(), new WindowResultFunction());


        SingleOutputStreamOperator<String> topN = categoryItemCnt.keyBy("windowEndTs")
                .process(new TopNKeyedProcessFunction(3));
        return null;

    }
}
