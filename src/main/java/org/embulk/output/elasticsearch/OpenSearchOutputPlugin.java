/*
 * Copyright 2015 The Embulk project
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

import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;

public class OpenSearchOutputPlugin
        extends OpenSearchOutputPluginBase<OpenSearchOutputPluginDelegate.PluginTask>
{
    public OpenSearchOutputPlugin()
    {
        super(CONFIG_MAPPER_FACTORY, OpenSearchOutputPluginDelegate.PluginTask.class, new OpenSearchOutputPluginDelegate());
    }

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
}
