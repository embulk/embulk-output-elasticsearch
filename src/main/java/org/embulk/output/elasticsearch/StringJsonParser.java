/*
 * Copyright 2016 The Embulk project
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.embulk.spi.DataException;

public class StringJsonParser {
    public StringJsonParser() {
        this.mapper = new ObjectMapper();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    }

    public ObjectNode parseJsonObject(final String jsonText) {
        final JsonNode node = this.parseJsonNode(jsonText);
        if (node.isObject()) {
            return (ObjectNode) node;
        } else {
            throw new DataException("Expected object node: " + jsonText);
        }
    }

    public ArrayNode parseJsonArray(final String jsonText) {
        final JsonNode node = this.parseJsonNode(jsonText);
        if (node.isArray()) {
            return (ArrayNode) node;
        } else {
            throw new DataException("Expected array node: " + jsonText);
        }
    }

    public JsonNode parseJsonNode(final String jsonText) {
        try {
            return mapper.readTree(jsonText);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final ObjectMapper mapper;
}
