package com.sitech.gmall.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sitech.gmall.logger.common.GmallConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogController {

    private Logger logger = LoggerFactory.getLogger(LogController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/log")
    public String sendLog(@RequestParam(name = "logString") String logStr) {

        JSONObject jo = JSON.parseObject(logStr);

        jo.put("ts", System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jo.toJSONString();
        logger.info(jo.toJSONString());

        // 2 推送到kafka
        if ("startup".equals(jo.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "ok";
    }


}
