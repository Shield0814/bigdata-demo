package com.sitech.gmall.brc.monitor;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public interface TableMonitor {

    //canal 客户端连接
    CanalConnector canalConn = null;

    //要监控的表名称
    String tableName = null;

    //要监控的数据变化类型: INSERT,UPDATE
    List<CanalEntry.EventType> dataEventType = null;

    //对监控到的事件的进行处理
    void handle();


}
