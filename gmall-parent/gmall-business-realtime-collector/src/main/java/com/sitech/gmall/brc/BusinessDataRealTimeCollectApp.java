package com.sitech.gmall.brc;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.sitech.gmall.brc.common.ConfigManager;
import com.sitech.gmall.brc.monitor.TableMonitor;
import com.sitech.gmall.brc.monitor.impl.OrderInfoTableMonitor;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public class BusinessDataRealTimeCollectApp {

    public static void main(String[] args) {

        CanalConnector canalConn = CanalConnectors.newSingleConnector(
                new InetSocketAddress(ConfigManager.get("canal.host"),
                        Integer.valueOf(ConfigManager.get("canal.port"))),
                ConfigManager.get("canal.destination"),
                ConfigManager.get("canal.username"),
                ConfigManager.get("canal.password")
        );
        ArrayList<CanalEntry.EventType> dataEventType = new ArrayList<>();
        dataEventType.add(CanalEntry.EventType.INSERT);
        String tableName = ConfigManager.get("monitor.table.order_info");
        TableMonitor monitor = new OrderInfoTableMonitor(canalConn, tableName, dataEventType);
        monitor.handle();
    }
}
