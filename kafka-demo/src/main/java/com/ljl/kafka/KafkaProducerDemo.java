package com.ljl.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaProducerDemo {

    public static void main(String[] args) throws InterruptedException {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata116:9092,bigdata117:9092,bigdata118:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.ACKS_CONFIG, "-1");

        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");//默认32M
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "5");//默认0
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); //默认16K
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576"); //默认10M
        //消息发送请求超时时间，必须大于副本同步回值超时时间，即：request.timeout.ms > replica.lag.time.max.ms
        //从而避免不必要的发送重试
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        //开启幂等性
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //保证消息顺序发送，但会影响消息发送的吞吐量
//        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1");
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);
        Random random = new Random();

        while (true) {

            producer.send(new ProducerRecord<String, String>("test", String.valueOf(random.nextInt(1000))
                    , String.valueOf(random.nextInt(1000))), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println(exception.getCause());
                    } else {
                        System.out.println(metadata.timestamp());
                    }
                }
            });

            TimeUnit.SECONDS.sleep(1);

        }

    }
}
