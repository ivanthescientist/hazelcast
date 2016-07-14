/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.container;

import com.hazelcast.jet.job.JobListener;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.io.ObjectReaderFactory;
import com.hazelcast.jet.io.ObjectWriterFactory;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ContainerContext implements ContainerDescriptor {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final JetTupleFactory tupleFactory;
    private final JobContext jobContext;
    private final ConcurrentMap<String, Accumulator> accumulatorMap;

    public ContainerContext(NodeEngine nodeEngine,
                                   JobContext jobContext,
                                   int id,
                                   Vertex vertex,
                                   JetTupleFactory tupleFactory) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.tupleFactory = tupleFactory;
        this.jobContext = jobContext;
        this.accumulatorMap = new ConcurrentHashMap<String, Accumulator>();
        jobContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    /**
         * @return - JET-application context;
         */
    public JobContext getJobContext() {
        return this.jobContext;
    }

    @Override
    public String getApplicationName() {
        return this.jobContext.getName();
    }

    @Override
    public int getID() {
        return this.id;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public DAG getDAG() {
        return this.jobContext.getDAG();
    }

    @Override
    public JetTupleFactory getTupleFactory() {
        return this.tupleFactory;
    }

    @Override
    public JobConfig getConfig() {
        return this.jobContext.getJobConfig();
    }

    @Override
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        this.jobContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerApplicationListener(JobListener jobListener) {
        this.jobContext.registerApplicationListener(jobListener);
    }

    @Override
    public <T> void putApplicationVariable(String variableName, T variable) {
        this.jobContext.putJobVariable(variableName, variable);
    }

    @Override
    public <T> T getApplicationVariable(String variableName) {
        return this.jobContext.getJobVariable(variableName);
    }

    public void cleanApplicationVariable(String variableName) {
        this.jobContext.cleanJobVariable(variableName);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.jobContext.getIOContext().getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.jobContext.getIOContext().getObjectWriterFactory();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return this.accumulatorMap.get(key);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String key,
                                                           Accumulator<V, R> accumulator) {
        this.accumulatorMap.put(key, accumulator);
    }
}
