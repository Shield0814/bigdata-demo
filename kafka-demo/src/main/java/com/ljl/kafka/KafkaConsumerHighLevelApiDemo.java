package com.ljl.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerHighLevelApiDemo {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata116:9092,bigdata117:9092,bigdata118:9092");
        //是否开启offset自动提交
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //是否排除内部主题
        config.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        //消费者组
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "topic__consumer_offsets_cgroup");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //最大拉取消息的字节数
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");
        //offset为空的话，offset重置策略
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        consumer.subscribe(Arrays.asList("test"));


        while (true) {
            List<PartitionInfo> partitions = consumer.partitionsFor("test");
            for (PartitionInfo pt : partitions) {
                TopicPartition tp = new TopicPartition(pt.topic(), pt.partition());
                OffsetAndMetadata offsetMeta = consumer.committed(tp);
                if (offsetMeta != null) {
                    System.out.println("上次消费到：" + pt.partition() + "," + offsetMeta.offset());
                } else {
                    System.out.println(pt.partition() + " offset 不存在，即该分区没有数据");
                }
            }

            System.out.println("=================================");
            ConsumerRecords<String, String> records = consumer.poll(10000);

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            while (iter.hasNext()) {
                ConsumerRecord<String, String> record = iter.next();
                System.out.println(record.partition() + "," + record.offset() + "," + record.key() + "," + record.value());
            }
            consumer.commitSync();

            TimeUnit.SECONDS.sleep(1);

        }


    }
}
