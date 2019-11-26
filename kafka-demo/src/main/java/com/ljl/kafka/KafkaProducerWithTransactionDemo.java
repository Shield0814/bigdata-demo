package com.ljl.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerWithTransactionDemo {

    public static void main(String[] args) {
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

        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transaction-id");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>("test_tx", Integer.toString(i), Integer.toString(i)));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }
}
