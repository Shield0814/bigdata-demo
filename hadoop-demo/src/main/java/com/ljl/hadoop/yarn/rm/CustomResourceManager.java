package com.ljl.hadoop.yarn.rm;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class CustomResourceManager implements ResourceScheduler {


    @Override
    public void setRMContext(RMContext rmContext) {

    }

    @Override
    public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {

    }

    @Override
    public void recover(RMStateStore.RMState state) throws Exception {

    }

    @Override
    public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) throws IOException {
        return null;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
        return null;
    }

    @Override
    public Resource getClusterResource() {
        return null;
    }

    @Override
    public Resource getMinimumResourceCapability() {
        return null;
    }

    @Override
    public Resource getMaximumResourceCapability() {
        return null;
    }

    @Override
    public Resource getMaximumResourceCapability(String queueName) {
        return null;
    }

    @Override
    public ResourceCalculator getResourceCalculator() {
        return null;
    }

    @Override
    public int getNumClusterNodes() {
        return 0;
    }

    @Override
    public Allocation allocate(ApplicationAttemptId appAttemptId, List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
        return null;
    }

    @Override
    public SchedulerNodeReport getNodeReport(NodeId nodeId) {
        return null;
    }

    @Override
    public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
        return null;
    }

    @Override
    public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
        return null;
    }

    @Override
    public QueueMetrics getRootQueueMetrics() {
        return null;
    }

    @Override
    public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl, String queueName) {
        return false;
    }

    @Override
    public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
        return null;
    }

    @Override
    public RMContainer getRMContainer(ContainerId containerId) {
        return null;
    }

    @Override
    public String moveApplication(ApplicationId appId, String newQueue) throws YarnException {
        return null;
    }

    @Override
    public void moveAllApps(String sourceQueue, String destQueue) throws YarnException {

    }

    @Override
    public void killAllAppsInQueue(String queueName) throws YarnException {

    }

    @Override
    public void removeQueue(String queueName) throws YarnException {

    }

    @Override
    public void addQueue(Queue newQueue) throws YarnException {

    }

    @Override
    public void setEntitlement(String queue, QueueEntitlement entitlement) throws YarnException {

    }

    @Override
    public Set<String> getPlanQueues() throws YarnException {
        return null;
    }

    @Override
    public EnumSet<YarnServiceProtos.SchedulerResourceTypes> getSchedulingResourceTypes() {
        return null;
    }

    @Override
    public void handle(SchedulerEvent event) {

    }
}
