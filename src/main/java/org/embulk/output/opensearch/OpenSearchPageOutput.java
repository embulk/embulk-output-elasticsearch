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

package org.embulk.output.opensearch;

import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.RestClientPageOutput;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.RecordExporter;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapperFactory;

public class OpenSearchPageOutput<T extends RestClientOutputTaskBase> extends RestClientPageOutput<T>
{
    public OpenSearchPageOutput(
            final ConfigMapperFactory configMapperFactory,
            final Class<T> taskClass,
            final T task,
            final RecordExporter recordExporter,
            final RecordBuffer recordBuffer,
            final Schema embulkSchema,
            final int taskIndex)
    {
        super(configMapperFactory, taskClass, task, recordExporter, recordBuffer, embulkSchema, taskIndex);
    }

    @Override
    public void add(final Page page)
    {
        super.add(page);

        page.release();
    }
}
