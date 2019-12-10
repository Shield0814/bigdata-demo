package com.sitech.gmall.brc.monitor.impl;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.sitech.gmall.brc.common.ConfigManager;
import com.sitech.gmall.brc.monitor.TableMonitor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OrderInfoTableMonitor implements TableMonitor {

    private Logger logger = LoggerFactory.getLogger(OrderInfoTableMonitor.class);
    private CanalConnector canalConn;

    private String tableName;

    private List<CanalEntry.EventType> dataEventType;

    private KafkaProducer<String, String> kafkaProducer;

    public OrderInfoTableMonitor(CanalConnector canalConn,
                                 String tableName,
                                 List<CanalEntry.EventType> dataEventType) {
        this.canalConn = canalConn;
        this.tableName = tableName;
        this.dataEventType = dataEventType;
        init();
    }

    /**
     * 初始化
     */
    void init() {
        canalConn.connect();
        canalConn.subscribe(tableName);
        canalConn.rollback();
        kafkaProducer = new KafkaProducer<>(ConfigManager.getAll());
        kafkaProducer.initTransactions();
    }

    @Override
    public void handle() {
        int batchSize = 1000;
        long sleepMs = 5000;

        while (true) {
            try {
                Message message = canalConn.getWithoutAck(batchSize);
                long batchId = message.getId();
                List<CanalEntry.Entry> entryList = message.getEntries();
                if (batchId == -1 || entryList == null || entryList.isEmpty()) {
                    //如果没有拉取到变化,等待sleepMs后再拉取
                    Thread.sleep(sleepMs);
                } else {
                    //如果拉取到数据，则进行处理
                    saveToKakfa(entryList);
                }
                canalConn.ack(batchId); // 提交确认
            } catch (Exception e) {
                logger.error("监控表 " + tableName + " 时发生错误", e);
                break;
            }
        }
        canalConn.disconnect();

    }


    /**
     * 保存订单表发生变化的数据到kafka
     *
     * @param entryList
     */
    void saveToKakfa(List<CanalEntry.Entry> entryList) {
        for (CanalEntry.Entry entry : entryList) {
            //这里只处理行数据发生变化的entry类型
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange = null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    logger.error("解析变化的行数据时发生异常", e);
                }

                //如果数据变化的类型时INSERT,UPDATE的化，把数据保存到kafka
                if (dataEventType.contains(rowChange.getEventType())) {

                    List<CanalEntry.RowData> rowDatas = rowChange.getRowDatasList();
                    try {
                        kafkaProducer.beginTransaction();
                        for (CanalEntry.RowData rowData : rowDatas) {
                            JSONObject jo = new JSONObject();
                            //把修改后的数据封装更一个json对象
                            List<CanalEntry.Column> afterData = rowData.getAfterColumnsList();
                            afterData.forEach(column -> {
                                jo.put(column.getName(), column.getValue());
                            });

                            ProducerRecord<String, String> record =
                                    new ProducerRecord<>(ConfigManager.get("kafka.topic"), jo.toJSONString());
                            kafkaProducer.send(record,
                                    ((metadata, exception) -> {
                                        if (exception != null) {
                                            logger.error("数据发送到kafka时失败", exception);
                                        }
                                    }));

                        }
                        kafkaProducer.commitTransaction();
                    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                        kafkaProducer.close();
                    } catch (KafkaException e) {
                        kafkaProducer.abortTransaction();
                    }

                }
            }
        }
    }
}
