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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * Nacos execute task execute engine.
 *
 * @author xiweng.yy
 */
public class NacosExecuteTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractExecuteTask> {

    private final TaskExecuteWorker[] executeWorkers;

    public NacosExecuteTaskExecuteEngine(String name, Logger logger) {
        this(name, logger, ThreadUtils.getSuitableThreadCount(1));
    }

    public NacosExecuteTaskExecuteEngine(String name, Logger logger, int dispatchWorkerCount) {
        super(logger);

        // 每个TaskExecuteWorker中有一个queue和一个InnerWorker，每个InnerWorker就是一个线程，会阻塞式的从queue中获取任务来执行
        // TaskExecuteWorker相当于一个简单版线程池
        // NacosExecuteTaskExecuteEngine中包含了dispatchWorkerCount个TaskExecuteWorker
        executeWorkers = new TaskExecuteWorker[dispatchWorkerCount];
        for (int mod = 0; mod < dispatchWorkerCount; ++mod) {
            executeWorkers[mod] = new TaskExecuteWorker(name, mod, dispatchWorkerCount, getEngineLog());
        }
    }

    @Override
    public int size() {
        int result = 0;
        for (TaskExecuteWorker each : executeWorkers) {
            result += each.pendingTaskCount();
        }
        return result;
    }

    @Override
    public boolean isEmpty() {
        return 0 == size();
    }

    @Override
    public void addTask(Object tag, AbstractExecuteTask task) {
        NacosTaskProcessor processor = getProcessor(tag);
        if (null != processor) {
            processor.process(task);
            return;
        }

        // 根据tag的hash值取模，得到对应的TaskExecuteWorker，用它来执行当前task，也相当于是异步执行task
        TaskExecuteWorker worker = getWorker(tag);
        worker.process(task);
    }

    private TaskExecuteWorker getWorker(Object tag) {
        int idx = (tag.hashCode() & Integer.MAX_VALUE) % workersCount();
        return executeWorkers[idx];
    }

    private int workersCount() {
        return executeWorkers.length;
    }

    @Override
    public AbstractExecuteTask removeTask(Object key) {
        throw new UnsupportedOperationException("ExecuteTaskEngine do not support remove task");
    }

    @Override
    public Collection<Object> getAllTaskKeys() {
        throw new UnsupportedOperationException("ExecuteTaskEngine do not support get all task keys");
    }

    @Override
    public void shutdown() throws NacosException {
        for (TaskExecuteWorker each : executeWorkers) {
            each.shutdown();
        }
    }

    /**
     * Get workers status.
     *
     * @return workers status string
     */
    public String workersStatus() {
        StringBuilder sb = new StringBuilder();
        for (TaskExecuteWorker worker : executeWorkers) {
            sb.append(worker.status()).append('\n');
        }
        return sb.toString();
    }
}
