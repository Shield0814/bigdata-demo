package com.ljl.hadoop.yarn.resourcetracker;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;

import java.io.IOException;

public interface ResourceTracker {

    RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest request)
            throws YarnException, IOException;

    NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
            throws YarnException, IOException;
}
