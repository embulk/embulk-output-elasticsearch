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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.JacksonValueLocator;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.base.restclient.record.ValueLocator;

public class JacksonServiceRecord extends ServiceRecord
{
    public JacksonServiceRecord(final ObjectNode record)
    {
        this.record = record;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends ServiceRecord.Builder
    {
        public Builder()
        {
            this.node = MAPPER.createObjectNode();
        }

        @Override
        public void reset()
        {
            this.node = MAPPER.createObjectNode();
        }

        @Override
        public void add(final ServiceValue serviceValue, final ValueLocator valueLocator)
        {
            final JacksonServiceValue jacksonServiceValue;
            try {
                jacksonServiceValue = (JacksonServiceValue) serviceValue;
            }
            catch (final ClassCastException ex) {
                throw new RuntimeException("Non-JacksonServiceValue is given to place in JacksonServiceRecord:", ex);
            }

            final JacksonValueLocator jacksonValueLocator;
            try {
                jacksonValueLocator = (JacksonValueLocator) valueLocator;
            }
            catch (final ClassCastException ex) {
                throw new RuntimeException("Non-JacksonValueLocator is used to place a value in JacksonServiceRecord:", ex);
            }

            jacksonValueLocator.placeValue(this.node, jacksonServiceValue.getInternalJsonNode());
        }

        @Override
        public ServiceRecord build()
        {
            final JacksonServiceRecord built = new JacksonServiceRecord(this.node);
            this.reset();
            return built;
        }

        private static final ObjectMapper MAPPER = new ObjectMapper();

        private ObjectNode node;
    }

    @Override
    public JacksonServiceValue getValue(final ValueLocator locator)
    {
        // Based on the implementation of |JacksonServiceResponseSchema|,
        // the |ValueLocator| should be always |JacksonValueLocator|.
        // The class cast should always success.
        //
        // NOTE: Just casting (with catching |ClassCastException|) is
        // faster than checking (instanceof) and then casting if
        // the |ClassCastException| never or hardly happens.
        final JacksonValueLocator jacksonLocator;
        try {
            jacksonLocator = (JacksonValueLocator) locator;
        }
        catch (final ClassCastException ex) {
            throw new RuntimeException("Non-JacksonValueLocator is used to locate a value in JacksonServiceRecord", ex);
        }
        return new JacksonServiceValue(jacksonLocator.seekValue(record));
    }

    @Override
    public String toString()
    {
        return this.record.toString();
    }

    ObjectNode getInternalJsonNode()
    {
        return this.record;
    }

    private final ObjectNode record;
}
