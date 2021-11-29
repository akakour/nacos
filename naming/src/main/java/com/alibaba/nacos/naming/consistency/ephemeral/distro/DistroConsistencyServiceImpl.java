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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 * <p>
 * Use a distro algorithm to divide data into many blocks. Each Nacos server node takes
 * responsibility for exactly one block of data. Each block of data is generated, removed
 * and synchronized by its responsible server. So every Nacos server only handles writings
 * for a subset of the total service data.
 * <p>
 * At mean time every Nacos server receives data sync of other Nacos server, so every Nacos
 * server will eventually have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private DataStore dataStore;

    @Autowired
    private TaskDispatcher taskDispatcher;

    @Autowired
    private Serializer serializer;

    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    private boolean initialized = false;

    private volatile Notifier notifier = new Notifier();

    private LoadDataTask loadDataTask = new LoadDataTask();

    private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    /**
     * springboot 容器初始化的过程中，调用init方法
     * 顺便
     * 1. nacos节点一致性，初始化服务信息同步
     * 2. 初始化监听器，notifier -> start,监听各个recoderlistener（service，servicemanage，switchservice），
     * notifier有个queue，循环异步执行各个监听器，
     * 如 instance的onchange事件，delete事件。
     * cluster的onchange，delete事件
     */
    @PostConstruct
    public void init() {
        // 执行一致性算法 loadDataTask，同步nacos节点
        GlobalExecutor.submit(loadDataTask);
        // 执行监听器 notifier
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    /**
     * nacos集群节点间，信息同步
     */
    private class LoadDataTask implements Runnable {

        @Override
        public void run() {
            try {
                /**
                 * 同步集群信息
                 */
                load();
               //load() 执行完后
                if (!initialized) {
                    // 单次延迟执行
                    GlobalExecutor.submit(this, globalConfig.getLoadDataRetryDelayMillis());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("load data failed.", e);
            }
        }
    }

    /**
     * 同步集群信息
     * @throws Exception
     */
    public void load() throws Exception {
        // 启动参数 nacos.standalone 默认为false，以集群方式启动
        if (SystemUtils.STANDALONE_MODE) {
            // nacos以单节点形式启动
            initialized = true;
            return;
        }
        // size = 1 means only myself in the list, we need at least one another server alive:
        // 检测nacos集群健康节点数：其他节点没有启动完成的时候，while死循环，本子线程阻塞。
        while (serverListManager.getHealthyServers().size() <= 1) {
            Thread.sleep(1000L);
            Loggers.DISTRO.info("waiting server list init...");
        }

        //获取nacos集群的健康的子节点
        for (Server server : serverListManager.getHealthyServers()) {
            //自己本身跳过
            if (NetUtils.localServer().equals(server.getKey())) {
                continue;
            }
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("sync from " + server);
            }
            // try sync data from remote server:
            // 从其他节点同步instance信息给自己
            if (syncAllDataFromRemote(server)) {
                initialized = true;
                return;
            }
        }
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        /**
         * 1. 将instance信息保存在datastore中，以便于集群间同步
         * 2. 给监听器发送onchange通知，根据key不同， 监听器执行
         *    1）instance change --> com.alibaba.nacos.naming.core.Service#onChange(java.lang.String, com.alibaba.nacos.naming.core.Instances)
         *    2）cluster change --> com.alibaba.nacos.naming.core.ServiceManager#onChange(java.lang.String, com.alibaba.nacos.naming.core.Service)
         */
        onPut(key, value);

        taskDispatcher.addTask(key);
    }

    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * 将instance信息存储到本地，并且发送监听器onchange通知
     *
     * @param key
     * @param value
     */
    public void onPut(String key, Record value) {

        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            datum.timestamp.incrementAndGet();
            // 存储到datastore对象中（本地）
            dataStore.put(key, datum);
        }

        if (!listeners.containsKey(key)) {
            return;
        }

        //给服务监听器添加一条onchange事件。
        // 异步处理。
        // 观察者设计模式的落地实现
        notifier.addTask(key, ApplyAction.CHANGE);
    }

    /**
     * 将instance信息从本地删除，并且发送监听器ondelete通知
     *
     * @param key
     */
    public void onRemove(String key) {

        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }

        // 监听器delete通知
        notifier.addTask(key, ApplyAction.DELETE);
    }

    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        if (syncChecksumTasks.containsKey(server)) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }

        syncChecksumTasks.put(server, "1");

        try {

            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }

                if (!dataStore.contains(entry.getKey()) ||
                    dataStore.get(entry.getKey()).value == null ||
                    !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            for (String key : dataStore.keys()) {

                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }

                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            }

            for (String key : toRemoveKeys) {
                onRemove(key);
            }

            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                byte[] result = NamingProxy.getData(toUpdateKeys, server);
                processData(result);
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }

    }

    /**
     * 从其他nacos集群节点同步信息
     *
     * @param server
     * @return
     */
    public boolean syncAllDataFromRemote(Server server) {

        try {
            // http 请求获取server节点的服务注册信息
            byte[] data = NamingProxy.getAllData(server.getKey());
            // 解析获取到的信息，同步信息
            processData(data);
            return true;
        } catch (Exception e) {
            Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
            return false;
        }
    }

    /**
     * 解析并同步集群服务注册信息
     * @param data
     * @throws Exception
     */
    public void processData(byte[] data) throws Exception {
        if (data.length > 0) {
            // 反序列化
            // -> com.alibaba.nacos.naming.cluster.transport.FastJsonSerializer.deserialize(byte[], com.alibaba.fastjson.TypeReference<T>)
            Map<String, Datum<Instances>> datumMap =
                serializer.deserializeMap(data, Instances.class);

            // 遍历，判断，同步集群
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue());

                //判断是否存在本节点的listeners 中，若已经存在就不同步该信息
                //往本节点注册的服务处理之后，会将Serviceid作为key添加进listeners
                // 理论上，新上线nacos节点，listeners只有一个com.alibaba.nacos.naming.domains.meta.的key
                if (!listeners.containsKey(entry.getKey())) {
                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();
                        //同步来的信息，由com.alibaba.nacos.naming.domains.meta.去做change事件通知
                        //     -> 将service注册到ServiceMap和datastore
                        listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0)
                            .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            //同步了信息之后
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        //给每个监听器发布一个通知，直接调用serivice/serviceManage的onchange方法
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // Update data store if listener executed successfully:
                // 最终新增到本节点的datastore,
                // 注意，这里还是在springboot ioc实例化的时候
                //   -> com.alibaba.nacos.naming.consistency.ephemeral.distro.DistroConsistencyServiceImpl.init
                //   所以此时nacos还没有启动完毕，不会接受服务注册，datastore不需要发布curd信息给其他节点
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            listeners.put(key, new CopyOnWriteArrayList<>());
        }

        if (listeners.get(key).contains(listener)) {
            return;
        }

        listeners.get(key).add(listener);
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

        /**
         * 往堵塞队列中添加一条动作信息
         * 该堵塞队列被Notifier 调度线程一直监控
         * Notifier#run()
         *
         * @param datumKey
         * @param action
         */
        public void addTask(String datumKey, ApplyAction action) {

            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            if (action == ApplyAction.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }
            // 往堵塞队列中塞入一条动作信息
            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            while (true) {
                try {

                    //以堵塞的方式（有数据就取，无数据就等待堵塞）获取数据
                    // Pair  ——> 一种键值对数据结构
                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    //
                    services.remove(datumKey);

                    int count = 0;

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            if (action == ApplyAction.CHANGE) {
                                listener.onChange(datumKey, dataStore.get(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                    }
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }
    }
}
