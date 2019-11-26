package com.ljl.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerApiTest {
    KafkaConsumer<String, String> consumer;

    /**
     * 初始化kafakConsumer
     */
    @Before
    public void before() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata116:9092,bigdata117:9092,bigdata118:9092");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "cosumer_api_test_group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800");
        consumer = new KafkaConsumer<>(config);


    }

    /**
     * 测试获取某个消费者消费某个TopicaPartition中的offset
     */
    @Test
    public void testCommitted() {
        consumer.subscribe(Arrays.asList("topic__consumer_offsets_cgroup"));
        List<PartitionInfo> partitions = consumer.partitionsFor("test");
        for (PartitionInfo pt : partitions) {
            TopicPartition tp = new TopicPartition(pt.topic(), pt.partition());
            OffsetAndMetadata offsetMeta = consumer.committed(tp);
            if (offsetMeta != null) {
                System.out.println(pt.partition() + "," + offsetMeta.metadata() + "," + offsetMeta.offset());
            } else {
                System.out.println(pt.partition() + " offset 不存在，即该分区没有数据");
            }

        }

    }

    /**
     * 测试获取topic对应的分区
     */
    @Test
    public void testPartitionsFor() {
        List<PartitionInfo> partitions = consumer.partitionsFor("__consumer_offsets");
        System.out.println("topic\tpartition\tleader\treplicas\tinSyncReplicas");
        for (PartitionInfo pt : partitions) {
            System.out.println(pt.topic() + "\t" + pt.partition() + "\t" + pt.leader().host()
                    + "\t" + pt.replicas() + "\t" + pt.inSyncReplicas());
        }
    }

    /**
     * 测试topic订阅
     */
    @Test
    public void testSubscribe() {
        consumer.subscribe(Arrays.asList("__consumer_offsets"));
    }
}
