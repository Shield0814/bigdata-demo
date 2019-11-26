package com.ljl.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义 kafka 生产者拦截器，给value前加一个时间戳
 * kafka生产者发送消息流程：
 * 拦截器 -> 序列化器 -> 分区器 -> RecordAccumulator -> sender线程发送到broker
 */
public class TimestampInterceptor implements ProducerInterceptor<String, String> {

    /**
     * kafka生产者发送消息前调用
     *
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        long currentTime = System.currentTimeMillis();
        return new ProducerRecord<>(record.key(), currentTime + "|" + record.value());
    }

    /**
     * 生产者定义了回调函数的话，该方法会在消息从RecordAccumulator成功发送到Kafka Broker之后，或者在发送过程中失败时调用。
     *
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 拦截器关闭时调用
     */
    @Override
    public void close() {

    }

    /**
     * 获取配置信息和初始化数据时调用
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
