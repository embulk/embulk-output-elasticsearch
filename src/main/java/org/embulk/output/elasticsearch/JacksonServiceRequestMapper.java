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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.embulk.base.restclient.ServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonValueLocator;
import org.embulk.base.restclient.record.EmbulkValueScope;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.base.restclient.record.ValueExporter;

/**
 * |JacksonServiceRequestMapper| represents which Embulk values are mapped into JSON, and
 * how the values are placed in a JSON-based request.
 */
public final class JacksonServiceRequestMapper extends ServiceRequestMapper<JacksonValueLocator>
{
    protected JacksonServiceRequestMapper(final List<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> map)
    {
        super(map);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public RecordExporter createRecordExporter()
    {
        final ArrayList<ValueExporter> listBuilder = new ArrayList<>();
        for (final Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : entries()) {
            listBuilder.add(createValueExporter(entry.getKey(), entry.getValue()));
        }
        return new RecordExporter(Collections.unmodifiableList(listBuilder), JacksonServiceRecord.builder());
    }

    public static final class Builder
    {
        private Builder()
        {
            this.mapBuilder = new ArrayList<>();
        }

        public synchronized JacksonServiceRequestMapper.Builder addNewObject(final JacksonValueLocator valueLocator)
        {
            this.mapBuilder.add(new AbstractMap.SimpleEntry<>(
                    new JacksonNewObjectScope(),
                    new ColumnOptions<JacksonValueLocator>(valueLocator)));
            return this;

        }

        public synchronized JacksonServiceRequestMapper.Builder add(
                final EmbulkValueScope scope,
                final JacksonValueLocator valueLocator)
        {
            this.mapBuilder.add(new AbstractMap.SimpleEntry<>(scope, new ColumnOptions<JacksonValueLocator>(valueLocator)));
            return this;
        }

        public JacksonServiceRequestMapper build()
        {
            final ArrayList<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> built = new ArrayList<>();
            for (final Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>> entry : this.mapBuilder) {
                built.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
            return new JacksonServiceRequestMapper(Collections.unmodifiableList(mapBuilder));
        }

        private final ArrayList<Map.Entry<EmbulkValueScope, ColumnOptions<JacksonValueLocator>>> mapBuilder;
    }

    private ValueExporter createValueExporter(
            final EmbulkValueScope embulkValueScope, final ColumnOptions<JacksonValueLocator> columnOptions)
    {
        final JacksonValueLocator locator = columnOptions.getValueLocator();
        return new ValueExporter(embulkValueScope, locator);
    }
}
