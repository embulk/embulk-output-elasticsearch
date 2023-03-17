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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.time.Instant;
import org.embulk.base.restclient.record.ServiceValue;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

/**
 * JacksonServiceValue represents a value in a JSON response to be converted to an Embulk column value.
 *
 * {@code JacksonServiceValue} depends on Jackson {@code JsonNode}'s {@code as*} methods if type
 * conversion is needed.
 *
 * For example with Jackson 2.5.0, the JSON below is recognized as {@code boolean} {@code true}.
 * <pre>{@code
 * { "flag": "true" }
 * }</pre>
 *
 * The other JSON below however cannot be recognized as Embulk's {@code boolean} {@code true}
 * because Jackson 2.5.0 recognizes only {@code "true"} and {@code "false"}.
 * <pre>{@code
 * { "flag": "True" }
 * }</pre>
 *
 * @see <a href="https://github.com/FasterXML/jackson-databind/blob/jackson-databind-2.5.0/src/main/java/com/fasterxml/jackson/databind/node/TextNode.java#L177-L189">TextNode#asBoolean</a>
 *
 * Implement another set of {@code ServiceValue} and {@code ServiceRecord} if a different style of
 * type conversion is required.
 */
public class JacksonServiceValue extends ServiceValue
{
    public JacksonServiceValue(final JsonNode value)
    {
        if (value == null) {
            this.value = NullNode.getInstance();
        }
        else {
            this.value = value;
        }
    }

    @Override
    public boolean isNull()
    {
        return value.isNull();
    }

    @Override
    public boolean booleanValue()
    {
        return value.asBoolean();
    }

    @Override
    public double doubleValue()
    {
        return value.asDouble();
    }

    @Override
    public Value jsonValue(final JsonParser jsonParser)
    {
        // TODO(dmikurube): Use jackson-datatype-msgpack.
        // See: https://github.com/embulk/embulk-base-restclient/issues/32
        // Using |JsonNode#toString| instead of |JsonNode#asText| so that an empty JSON value can be parsed.
        // |asText| converts an empty |JsonNode| to "" while |toString| converts to "{}".
        return jsonParser.parse(value.toString());
    }

    @Override
    public long longValue()
    {
        return value.asLong();
    }

    @Override
    public String stringValue()
    {
        return value.asText();
    }

    @Override
    public Instant timestampValue(final TimestampFormatter timestampFormatter)
    {
        return timestampFormatter.parse(value.asText());
    }

    JsonNode getInternalJsonNode()
    {
        return this.value;
    }

    private final JsonNode value;
}
