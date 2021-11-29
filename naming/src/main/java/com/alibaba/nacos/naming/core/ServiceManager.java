/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Core manager storing all services in Nacos
 *
 * @author nkorange
 */
@Component
@DependsOn("nacosApplicationContext")
public class ServiceManager implements RecordListener<Service> {

    /**
     * Map<namespace, Map<group::serviceName, Service>>
     */
    private Map<String, Map<String, Service>> serviceMap = new ConcurrentHashMap<>();

    private LinkedBlockingDeque<ServiceKey> toBeUpdatedServicesQueue = new LinkedBlockingDeque<>(1024 * 1024);

    private Synchronizer synchronizer = new ServiceStatusSynchronizer();

    private final Lock lock = new ReentrantLock();

    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private PushService pushService;

    @Autowired
    private RaftPeerSet raftPeerSet;

    @Value("${nacos.naming.empty-service.auto-clean:false}")
    private boolean emptyServiceAutoClean;

    private int maxFinalizeCount = 3;

    private final Object putServiceLock = new Object();

    @Value("${nacos.naming.empty-service.clean.initial-delay-ms:60000}")
    private int cleanEmptyServiceDelay;

    @Value("${nacos.naming.empty-service.clean.period-time-ms:20000}")
    private int cleanEmptyServicePeriod;

    @PostConstruct
    public void init() {

        UtilsAndCommons.SERVICE_SYNCHRONIZATION_EXECUTOR.schedule(new ServiceReporter(), 60000, TimeUnit.MILLISECONDS);

        UtilsAndCommons.SERVICE_UPDATE_EXECUTOR.submit(new UpdatedServiceProcessor());

        if (emptyServiceAutoClean) {

            Loggers.SRV_LOG.info("open empty service auto clean job, initialDelay : {} ms, period : {} ms", cleanEmptyServiceDelay, cleanEmptyServicePeriod);

            // delay 60s, period 20s;

            // This task is not recommended to be performed frequently in order to avoid
            // the possibility that the service cache information may just be deleted
            // and then created due to the heartbeat mechanism

            GlobalExecutor.scheduleServiceAutoClean(new EmptyServiceAutoClean(), cleanEmptyServiceDelay, cleanEmptyServicePeriod);
        }

        try {
            Loggers.SRV_LOG.info("listen for service meta change");
            consistencyService.listen(KeyBuilder.SERVICE_META_KEY_PREFIX, this);
        } catch (NacosException e) {
            Loggers.SRV_LOG.error("listen for service meta change failed!");
        }
    }

    public Map<String, Service> chooseServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    public void addUpdatedService2Queue(String namespaceId, String serviceName, String serverIP, String checksum) {
        lock.lock();
        try {
            toBeUpdatedServicesQueue.offer(new ServiceKey(namespaceId, serviceName, serverIP, checksum), 5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            toBeUpdatedServicesQueue.poll();
            toBeUpdatedServicesQueue.add(new ServiceKey(namespaceId, serviceName, serverIP, checksum));
            Loggers.SRV_LOG.error("[DOMAIN-STATUS] Failed to add service to be updatd to queue.", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public void onChange(String key, Service service) throws Exception {
        try {
            if (service == null) {
                Loggers.SRV_LOG.warn("received empty push from raft, key: {}", key);
                return;
            }

            if (StringUtils.isBlank(service.getNamespaceId())) {
                service.setNamespaceId(Constants.DEFAULT_NAMESPACE_ID);
            }

            Loggers.RAFT.info("[RAFT-NOTIFIER] datum is changed, key: {}, value: {}", key, service);

            Service oldDom = getService(service.getNamespaceId(), service.getName());

            if (oldDom != null) {
                oldDom.update(service);
                // re-listen to handle the situation when the underlying listener is removed:
                consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), oldDom);
                consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), oldDom);
            } else {
                putServiceAndInit(service);
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[NACOS-SERVICE] error while processing service update", e);
        }
    }

    @Override
    public void onDelete(String key) throws Exception {
        String namespace = KeyBuilder.getNamespace(key);
        String name = KeyBuilder.getServiceName(key);
        Service service = chooseServiceMap(namespace).get(name);
        Loggers.RAFT.info("[RAFT-NOTIFIER] datum is deleted, key: {}", key);

        if (service != null) {
            service.destroy();
            consistencyService.remove(KeyBuilder.buildInstanceListKey(namespace, name, true));

            consistencyService.remove(KeyBuilder.buildInstanceListKey(namespace, name, false));

            consistencyService.unlisten(KeyBuilder.buildServiceMetaKey(namespace, name), service);
            Loggers.SRV_LOG.info("[DEAD-SERVICE] {}", service.toJSON());
        }

        chooseServiceMap(namespace).remove(name);
    }

    private class UpdatedServiceProcessor implements Runnable {
        //get changed service from other server asynchronously
        @Override
        public void run() {
            ServiceKey serviceKey = null;

            try {
                while (true) {
                    try {
                        serviceKey = toBeUpdatedServicesQueue.take();
                    } catch (Exception e) {
                        Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while taking item from LinkedBlockingDeque.");
                    }

                    if (serviceKey == null) {
                        continue;
                    }
                    GlobalExecutor.submitServiceUpdate(new ServiceUpdater(serviceKey));
                }
            } catch (Exception e) {
                Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while update service: {}", serviceKey, e);
            }
        }
    }

    private class ServiceUpdater implements Runnable {

        String namespaceId;
        String serviceName;
        String serverIP;

        public ServiceUpdater(ServiceKey serviceKey) {
            this.namespaceId = serviceKey.getNamespaceId();
            this.serviceName = serviceKey.getServiceName();
            this.serverIP = serviceKey.getServerIP();
        }

        @Override
        public void run() {
            try {
                updatedHealthStatus(namespaceId, serviceName, serverIP);
            } catch (Exception e) {
                Loggers.SRV_LOG.warn("[DOMAIN-UPDATER] Exception while update service: {} from {}, error: {}",
                    serviceName, serverIP, e);
            }
        }
    }

    public int getPagedClusterState(String namespaceId, int startPage, int pageSize, String keyword, List<RaftPeer> raftPeerList) {

        List<RaftPeer> matchList = new ArrayList<>();
        RaftPeer localRaftPeer = raftPeerSet.local();
        matchList.add(localRaftPeer);
        Set<String> otherServerSet = raftPeerSet.allServersWithoutMySelf();
        if (null != otherServerSet && otherServerSet.size() > 0) {
            for (String server : otherServerSet) {
                String path = UtilsAndCommons.NACOS_NAMING_OPERATOR_CONTEXT + UtilsAndCommons.NACOS_NAMING_CLUSTER_CONTEXT + "/state";
                Map<String, String> params = Maps.newHashMapWithExpectedSize(2);
                try {
                    String content = NamingProxy.reqCommon(path, params, server, false);
                    if (!StringUtils.EMPTY.equals(content)) {
                        RaftPeer raftPeer = JSONObject.parseObject(content, RaftPeer.class);
                        if (null != raftPeer) {
                            matchList.add(raftPeer);
                        }
                    }
                } catch (Exception e) {
                    Loggers.SRV_LOG.warn("[QUERY-CLUSTER-STATE] Exception while query cluster state from {}, error: {}",
                        server, e);
                }
            }
        }
        List<RaftPeer> tempList = new ArrayList<>();
        if (StringUtils.isNotBlank(keyword)) {
            for (RaftPeer raftPeer : matchList) {
                String ip = raftPeer.ip.split(":")[0];
                if (keyword.equals(ip)) {
                    tempList.add(raftPeer);
                }
            }
            matchList = tempList;
        }

        if (pageSize >= matchList.size()) {
            raftPeerList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            raftPeerList.add(matchList.get(i));

            if (raftPeerList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    public RaftPeer getMySelfClusterState() {
        return raftPeerSet.local();
    }

    public void updatedHealthStatus(String namespaceId, String serviceName, String serverIP) {
        Message msg = synchronizer.get(serverIP, UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
        JSONObject serviceJson = JSON.parseObject(msg.getData());

        JSONArray ipList = serviceJson.getJSONArray("ips");
        Map<String, String> ipsMap = new HashMap<>(ipList.size());
        for (int i = 0; i < ipList.size(); i++) {

            String ip = ipList.getString(i);
            String[] strings = ip.split("_");
            ipsMap.put(strings[0], strings[1]);
        }

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            return;
        }

        boolean changed = false;

        List<Instance> instances = service.allIPs();
        for (Instance instance : instances) {

            boolean valid = Boolean.parseBoolean(ipsMap.get(instance.toIPAddr()));
            if (valid != instance.isHealthy()) {
                changed = true;
                instance.setHealthy(valid);
                Loggers.EVT_LOG.info("{} {SYNC} IP-{} : {}:{}@{}",
                    serviceName, (instance.isHealthy() ? "ENABLED" : "DISABLED"),
                    instance.getIp(), instance.getPort(), instance.getClusterName());
            }
        }

        if (changed) {
            pushService.serviceChanged(service);
        }

        StringBuilder stringBuilder = new StringBuilder();
        List<Instance> allIps = service.allIPs();
        for (Instance instance : allIps) {
            stringBuilder.append(instance.toIPAddr()).append("_").append(instance.isHealthy()).append(",");
        }

        if (changed && Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[HEALTH-STATUS-UPDATED] namespace: {}, service: {}, ips: {}",
                service.getNamespaceId(), service.getName(), stringBuilder.toString());
        }

    }

    public Set<String> getAllServiceNames(String namespaceId) {
        return serviceMap.get(namespaceId).keySet();
    }

    public Map<String, Set<String>> getAllServiceNames() {

        Map<String, Set<String>> namesMap = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            namesMap.put(namespaceId, serviceMap.get(namespaceId).keySet());
        }
        return namesMap;
    }

    public Set<String> getAllNamespaces() {
        return serviceMap.keySet();
    }

    public List<String> getAllServiceNameList(String namespaceId) {
        if (chooseServiceMap(namespaceId) == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(chooseServiceMap(namespaceId).keySet());
    }

    public Map<String, Set<Service>> getResponsibleServices() {
        Map<String, Set<Service>> result = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            result.put(namespaceId, new HashSet<>());
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                Service service = entry.getValue();
                if (distroMapper.responsible(entry.getKey())) {
                    result.get(namespaceId).add(service);
                }
            }
        }
        return result;
    }

    public int getResponsibleServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                if (distroMapper.responsible(entry.getKey())) {
                    serviceCount++;
                }
            }
        }
        return serviceCount;
    }

    public int getResponsibleInstanceCount() {
        Map<String, Set<Service>> responsibleServices = getResponsibleServices();
        int count = 0;
        for (String namespaceId : responsibleServices.keySet()) {
            for (Service service : responsibleServices.get(namespaceId)) {
                count += service.allIPs().size();
            }
        }

        return count;
    }

    public void easyRemoveService(String namespaceId, String serviceName) throws Exception {

        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            throw new IllegalArgumentException("specified service not exist, serviceName : " + serviceName);
        }

        consistencyService.remove(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName));
    }

    public void addOrReplaceService(Service service) throws NacosException {
        consistencyService.put(KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName()), service);
    }

    public void createEmptyService(String namespaceId, String serviceName, boolean local) throws NacosException {
        createServiceIfAbsent(namespaceId, serviceName, local, null);
    }

    /**
     * 创建服务
     *
     * @param namespaceId
     * @param serviceName
     * @param local
     * @param cluster
     * @throws NacosException
     */
    public void createServiceIfAbsent(String namespaceId, String serviceName, boolean local, Cluster cluster) throws NacosException {
        //Q：为什么要【判断是否以及存在？理论上不是肯定不存在？
        //A: servicemanage单例。多线程不安全。有可能心跳早于注册（网络波动，注册失败），心跳处理里面，如果发现本服务没有注册过，会先注册在心跳（更新最终修改时间）
        Service service = getService(namespaceId, serviceName);
        if (service == null) {

            Loggers.SRV_LOG.info("creating empty service {}:{}", namespaceId, serviceName);
            //new 一个service，并设置属性
            service = new Service();
            service.setName(serviceName);
            service.setNamespaceId(namespaceId);
            service.setGroupName(NamingUtils.getGroupName(serviceName));
            // now validate the service. if failed, exception will be thrown
            service.setLastModifiedMillis(System.currentTimeMillis());
            //重新计算整个nacos注册的实例的ip字符串的md5，应该是为了安全
            service.recalculateChecksum();
            if (cluster != null) {
                cluster.setService(service);
                service.getClusterMap().put(cluster.getName(), cluster);
            }
            service.validate();

            // 将service put到servicemap并且启动service的instance健康检查，并且将service添加进事件监听器，便于调用service的onchang事件等
            putServiceAndInit(service);
            if (!local) {
                //持久化
                addOrReplaceService(service);
            }
        }
    }

    /**
     * Register an instance to a service in AP mode.
     * <p>
     * This method creates service or cluster silently if they don't exist.
     *
     * @param namespaceId id of namespace
     * @param serviceName service name
     * @param instance    instance to register
     * @throws Exception any error occurred in the process
     */
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        // 只创建一个service空对象，没有添加instance实例。注意理解servicemap的层级结构
        createEmptyService(namespaceId, serviceName, instance.isEphemeral());
        // 得到service
        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }
        // 往service里添加实例（ip等）
        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        if (!service.allIPs().contains(instance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "instance not exist: " + instance);
        }

        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    /**
     * 添加实例
     *
     * @param namespaceId
     * @param serviceName
     * @param ephemeral
     * @param ips
     * @throws NacosException
     */
    public void addInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips) throws NacosException {

        //根据是否是临时（ap）还是非临时（cp），创建key
        //ap： com.alibaba.nacos.naming.iplist.ephemeral.namespaceId(public)##serviceName
        //cp： com.alibaba.nacos.naming.iplist.namespaceId(public)##serviceName
        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);

        Service service = getService(namespaceId, serviceName);

        synchronized (service) {
            //添加instance信息
            List<Instance> instanceList = addIpAddresses(service, ephemeral, ips);

            Instances instances = new Instances();
            instances.setInstanceList(instanceList);

            //集群一致性 TODO，udp通知对该服务感兴趣的client
            consistencyService.put(key, instances);
        }
    }

    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips) throws NacosException {
        Service service = getService(namespaceId, serviceName);

        synchronized (service) {
            removeInstance(namespaceId, serviceName, ephemeral, service, ips);
        }
    }

    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Service service, Instance... ips) throws NacosException {

        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);

        List<Instance> instanceList = substractIpAddresses(service, ephemeral, ips);

        Instances instances = new Instances();
        instances.setInstanceList(instanceList);

        consistencyService.put(key, instances);
    }

    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port) {
        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            return null;
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            return null;
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                return instance;
            }
        }

        return null;
    }

    /**
     * 更新同一个service的instance信息，
     * 追加instance或者删除instance
     *
     * @param service
     * @param action
     * @param ephemeral
     * @param ips
     * @return
     * @throws NacosException
     */
    public List<Instance> updateIpAddresses(Service service, String action, boolean ephemeral, Instance... ips) throws NacosException {

        //获取本地已经存在的service集群数据，比如订单服务。这里是从datastore中取得（datastore，用于nacos集群间同步数据，同步来的数据也是存放在datastore）
        Datum datum = consistencyService.get(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), ephemeral));

        List<Instance> currentIPs = service.allIPs(ephemeral);
        //现在的instance及其id
        Map<String, Instance> currentInstances = new HashMap<>(currentIPs.size());
        Set<String> currentInstanceIds = Sets.newHashSet();

        for (Instance instance : currentIPs) {
            currentInstances.put(instance.toIPAddr(), instance);
            currentInstanceIds.add(instance.getInstanceId());
        }

        Map<String, Instance> instanceMap;
        if (datum != null) {
            //已经有注册过相同服务（比如订单服务）
            instanceMap = setValid(((Instances) datum.value).getInstanceList(), currentInstances);
        } else {
            instanceMap = new HashMap<>(ips.length);
        }

        for (Instance instance : ips) {
            if (!service.getClusterMap().containsKey(instance.getClusterName())) {
                Cluster cluster = new Cluster(instance.getClusterName(), service);
                cluster.init();
                service.getClusterMap().put(instance.getClusterName(), cluster);
                Loggers.SRV_LOG.warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                    instance.getClusterName(), instance.toJSON());
            }

            if (UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE.equals(action)) {
                //删除instance实例，离线
                instanceMap.remove(instance.getDatumKey());
            } else {
                //生成新instanceid 可以基于雪花算法，有实现
                instance.setInstanceId(instance.generateInstanceId(currentInstanceIds));
                //添加新实例
                instanceMap.put(instance.getDatumKey(), instance);
            }

        }

        if (instanceMap.size() <= 0 && UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD.equals(action)) {
            throw new IllegalArgumentException("ip list can not be empty, service: " + service.getName() + ", ip list: "
                + JSON.toJSONString(instanceMap.values()));
        }

        return new ArrayList<>(instanceMap.values());
    }

    /**
     * 删除instance
     * @param service
     * @param ephemeral
     * @param ips
     * @return
     * @throws NacosException
     */
    public List<Instance> substractIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE, ephemeral, ips);
    }

    /**
     *  添加instance
     * @param service
     * @param ephemeral
     * @param ips
     * @return
     * @throws NacosException
     */
    public List<Instance> addIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD, ephemeral, ips);
    }

    private Map<String, Instance> setValid(List<Instance> oldInstances, Map<String, Instance> map) {

        Map<String, Instance> instanceMap = new HashMap<>(oldInstances.size());
        for (Instance instance : oldInstances) {
            Instance instance1 = map.get(instance.toIPAddr());
            if (instance1 != null) {
                instance.setHealthy(instance1.isHealthy());
                instance.setLastBeat(instance1.getLastBeat());
            }
            instanceMap.put(instance.getDatumKey(), instance);
        }
        return instanceMap;
    }

    public Service getService(String namespaceId, String serviceName) {
        if (serviceMap.get(namespaceId) == null) {
            return null;
        }
        return chooseServiceMap(namespaceId).get(serviceName);
    }

    public boolean containService(String namespaceId, String serviceName) {
        return getService(namespaceId, serviceName) != null;
    }

    /**
     * 将service存放进本地servicemap
     *
     * @param service
     */
    public void putService(Service service) {
        if (!serviceMap.containsKey(service.getNamespaceId())) {
            synchronized (putServiceLock) {
                //Q：为什么还要在判断一次？
                //A：servicemanage单例，多线程下，如果此时心跳请求（http）先于服务注册（网络波动，服务注册失败），此时可能已经有数据。
                if (!serviceMap.containsKey(service.getNamespaceId())) {
                    serviceMap.put(service.getNamespaceId(), new ConcurrentHashMap<>(16));
                }
            }
        }
        serviceMap.get(service.getNamespaceId()).put(service.getName(), service);
    }

    /**
     * 1. 将service存放进servicemap
     * 2. 启动心跳检查任务
     * 3. 基于观察者设计模式，添加事件监听器 （nacos设计的精髓）
     *
     * @param service
     * @throws NacosException
     */
    private void putServiceAndInit(Service service) throws NacosException {
        //将service存放进servicemap
        putService(service);
        //启动service心跳检查任务（service下所有instance的健康检查）
        service.init();
        //添加service级别的事件监听器  --->  基于观察者设计模式，反向调用Record接口的实现（service，instance，switchservice）的onchange等事件
        //ap-->集群采用ali内部的distro协议。----->com.alibaba.nacos.naming.consistency.ephemeral.distro.DistroConsistencyServiceImpl
        consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), service);
        //cp-->集群采用raft协议。-----> com.alibaba.nacos.naming.consistency.persistent.raft.RaftConsistencyServiceImpl
        consistencyService.listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), service);
        Loggers.SRV_LOG.info("[NEW-SERVICE] {}", service.toJSON());
    }


    public List<Service> searchServices(String namespaceId, String regex) {
        List<Service> result = new ArrayList<>();
        for (Map.Entry<String, Service> entry : chooseServiceMap(namespaceId).entrySet()) {
            Service service = entry.getValue();
            String key = service.getName() + ":" + ArrayUtils.toString(service.getOwners());
            if (key.matches(regex)) {
                result.add(service);
            }
        }

        return result;
    }

    public int getServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            serviceCount += serviceMap.get(namespaceId).size();
        }
        return serviceCount;
    }

    public int getInstanceCount() {
        int total = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Service service : serviceMap.get(namespaceId).values()) {
                total += service.allIPs().size();
            }
        }
        return total;
    }

    public Map<String, Service> getServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    public int getPagedService(String namespaceId, int startPage, int pageSize, String param, String containedInstance, List<Service> serviceList, boolean hasIpCount) {

        List<Service> matchList;

        if (chooseServiceMap(namespaceId) == null) {
            return 0;
        }

        if (StringUtils.isNotBlank(param)) {
            StringJoiner regex = new StringJoiner(Constants.SERVICE_INFO_SPLITER);
            for (String s : param.split(Constants.SERVICE_INFO_SPLITER)) {
                regex.add(StringUtils.isBlank(s) ? Constants.ANY_PATTERN : Constants.ANY_PATTERN + s + Constants.ANY_PATTERN);
            }
            matchList = searchServices(namespaceId, regex.toString());
        } else {
            matchList = new ArrayList<>(chooseServiceMap(namespaceId).values());
        }

        if (!CollectionUtils.isEmpty(matchList) && hasIpCount) {
            matchList = matchList.stream().filter(s -> !CollectionUtils.isEmpty(s.allIPs())).collect(Collectors.toList());
        }

        if (StringUtils.isNotBlank(containedInstance)) {

            boolean contained;
            for (int i = 0; i < matchList.size(); i++) {
                Service service = matchList.get(i);
                contained = false;
                List<Instance> instances = service.allIPs();
                for (Instance instance : instances) {
                    if (containedInstance.contains(":")) {
                        if (StringUtils.equals(instance.getIp() + ":" + instance.getPort(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    } else {
                        if (StringUtils.equals(instance.getIp(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    }
                }
                if (!contained) {
                    matchList.remove(i);
                    i--;
                }
            }
        }

        if (pageSize >= matchList.size()) {
            serviceList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            serviceList.add(matchList.get(i));

            if (serviceList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    public static class ServiceChecksum {

        public String namespaceId;
        public Map<String, String> serviceName2Checksum = new HashMap<String, String>();

        public ServiceChecksum() {
            this.namespaceId = Constants.DEFAULT_NAMESPACE_ID;
        }

        public ServiceChecksum(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        public void addItem(String serviceName, String checksum) {
            if (StringUtils.isEmpty(serviceName) || StringUtils.isEmpty(checksum)) {
                Loggers.SRV_LOG.warn("[DOMAIN-CHECKSUM] serviceName or checksum is empty,serviceName: {}, checksum: {}",
                    serviceName, checksum);
                return;
            }
            serviceName2Checksum.put(serviceName, checksum);
        }
    }


    private class EmptyServiceAutoClean implements Runnable {

        @Override
        public void run() {

            // Parallel flow opening threshold

            int parallelSize = 100;

            serviceMap.forEach((namespace, stringServiceMap) -> {
                Stream<Map.Entry<String, Service>> stream = null;
                if (stringServiceMap.size() > parallelSize) {
                    stream = stringServiceMap.entrySet().parallelStream();
                } else {
                    stream = stringServiceMap.entrySet().stream();
                }
                stream
                    .filter(entry -> {
                        final String serviceName = entry.getKey();
                        return distroMapper.responsible(serviceName);
                    })
                    .forEach(entry -> stringServiceMap.computeIfPresent(entry.getKey(), (serviceName, service) -> {
                        if (service.isEmpty()) {

                            // To avoid violent Service removal, the number of times the Service
                            // experiences Empty is determined by finalizeCnt, and if the specified
                            // value is reached, it is removed

                            if (service.getFinalizeCount() > maxFinalizeCount) {
                                Loggers.SRV_LOG.warn("namespace : {}, [{}] services are automatically cleaned",
                                    namespace, serviceName);
                                try {
                                    easyRemoveService(namespace, serviceName);
                                } catch (Exception e) {
                                    Loggers.SRV_LOG.error("namespace : {}, [{}] services are automatically clean has " +
                                        "error : {}", namespace, serviceName, e);
                                }
                            }

                            service.setFinalizeCount(service.getFinalizeCount() + 1);

                            Loggers.SRV_LOG.debug("namespace : {}, [{}] The number of times the current service experiences " +
                                "an empty instance is : {}", namespace, serviceName, service.getFinalizeCount());
                        } else {
                            service.setFinalizeCount(0);
                        }
                        return service;
                    }));
            });
        }
    }

    private class ServiceReporter implements Runnable {

        @Override
        public void run() {
            try {

                Map<String, Set<String>> allServiceNames = getAllServiceNames();

                if (allServiceNames.size() <= 0) {
                    //ignore
                    return;
                }

                for (String namespaceId : allServiceNames.keySet()) {

                    ServiceChecksum checksum = new ServiceChecksum(namespaceId);

                    for (String serviceName : allServiceNames.get(namespaceId)) {
                        if (!distroMapper.responsible(serviceName)) {
                            continue;
                        }

                        Service service = getService(namespaceId, serviceName);

                        if (service == null || service.isEmpty()) {
                            continue;
                        }

                        service.recalculateChecksum();

                        checksum.addItem(serviceName, service.getChecksum());
                    }

                    Message msg = new Message();

                    msg.setData(JSON.toJSONString(checksum));

                    List<Server> sameSiteServers = serverListManager.getServers();

                    if (sameSiteServers == null || sameSiteServers.size() <= 0) {
                        return;
                    }

                    for (Server server : sameSiteServers) {
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }
                        synchronizer.send(server.getKey(), msg);
                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[DOMAIN-STATUS] Exception while sending service status", e);
            } finally {
                UtilsAndCommons.SERVICE_SYNCHRONIZATION_EXECUTOR.schedule(this, switchDomain.getServiceStatusSynchronizationPeriodMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private static class ServiceKey {
        private String namespaceId;
        private String serviceName;
        private String serverIP;
        private String checksum;

        public String getChecksum() {
            return checksum;
        }

        public String getServerIP() {
            return serverIP;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public ServiceKey(String namespaceId, String serviceName, String serverIP, String checksum) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.serverIP = serverIP;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}
