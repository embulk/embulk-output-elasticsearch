/*
 * Copyright 2017 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.elasticsearch;

import org.embulk.base.restclient.EmbulkDataEgestable;
import org.embulk.base.restclient.OutputTaskValidatable;
import org.embulk.base.restclient.RecordBufferBuildable;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.RestClientPluginBase;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.ServiceRequestMapperBuildable;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapperFactory;

import java.util.List;

public class OpenSearchOutputPluginBase<T extends RestClientOutputTaskBase>
        extends RestClientPluginBase<T>
        implements OutputPlugin
{
    protected OpenSearchOutputPluginBase(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final EmbulkDataEgestable<T> embulkDataEgester,
            final RecordBufferBuildable<T> recordBufferBuilder,
            final OutputTaskValidatable<T> outputTaskValidator,
            final ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder)
    {
        super(configMapperFactory);
        this.taskClass = taskClass;
        this.embulkDataEgester = embulkDataEgester;
        this.recordBufferBuilder = recordBufferBuilder;
        this.outputTaskValidator = outputTaskValidator;
        this.serviceRequestMapperBuilder = serviceRequestMapperBuilder;
    }

    protected OpenSearchOutputPluginBase(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final RestClientOutputPluginDelegate<T> delegate)
    {
        this(configMapperFactory, taskClass, delegate, delegate, delegate, delegate);
    }

    @Override
    public ConfigDiff transaction(
            final ConfigSource config, final Schema schema, final int taskCount, final OutputPlugin.Control control)
    {
        final T task = loadConfig(config, this.taskClass);
        this.outputTaskValidator.validateOutputTask(task, schema, taskCount);
        return resume(task.toTaskSource(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(
            final TaskSource taskSource, final Schema schema, final int taskCount, final OutputPlugin.Control control)
    {
        final T task = loadTask(taskSource, this.taskClass);
        final List<TaskReport> taskReports = control.run(taskSource);
        return this.embulkDataEgester.egestEmbulkData(task, schema, taskCount, taskReports);
    }

    @Override
    public void cleanup(
            final TaskSource taskSource, final Schema schema, final int taskCount, final List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(final TaskSource taskSource, final Schema schema, final int taskIndex)
    {
        final T task = loadTask(taskSource, this.taskClass);
        final ServiceRequestMapper<? extends ValueLocator> serviceRequestMapper =
                this.serviceRequestMapperBuilder.buildServiceRequestMapper(task);
        return new OpenSearchPageOutput<T>(this.getConfigMapperFactory(),
                                           this.taskClass,
                                           task,
                                           serviceRequestMapper.createRecordExporter(),
                                           this.recordBufferBuilder.buildRecordBuffer(task, schema, taskIndex),
                                           schema,
                                           taskIndex);
    }

    private final Class<T> taskClass;
    private final EmbulkDataEgestable<T> embulkDataEgester;
    private final RecordBufferBuildable<T> recordBufferBuilder;
    private final OutputTaskValidatable<T> outputTaskValidator;
    private final ServiceRequestMapperBuildable<T> serviceRequestMapperBuilder;
}
