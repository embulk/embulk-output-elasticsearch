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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.record.SinglePageRecordReader;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;

public class JacksonAllInObjectScope extends JacksonObjectScopeBase
{
    public JacksonAllInObjectScope()
    {
        this(null, false);
    }

    public JacksonAllInObjectScope(final boolean fillsJsonNullForEmbulkNull)
    {
        this(null, fillsJsonNullForEmbulkNull);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter)
    {
        this(timestampFormatter, false);
    }

    public JacksonAllInObjectScope(final TimestampFormatter timestampFormatter, final boolean fillsJsonNullForEmbulkNull)
    {
        this.timestampFormatter = timestampFormatter;
        this.jsonParser = new StringJsonParser();
        this.fillsJsonNullForEmbulkNull = fillsJsonNullForEmbulkNull;
    }

    @Override
    public ObjectNode scopeObject(final SinglePageRecordReader singlePageRecordReader)
    {
        final ObjectNode resultObject = OBJECT_MAPPER.createObjectNode();

        singlePageRecordReader.getSchema().visitColumns(new ColumnVisitor() {
                @Override
                public void booleanColumn(final Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getBoolean(column));
                    }
                    else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void longColumn(final Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getLong(column));
                    }
                    else if (fillsJsonNullForEmbulkNull)  {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void doubleColumn(final Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getDouble(column));
                    }
                    else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void stringColumn(final Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        resultObject.put(column.getName(), singlePageRecordReader.getString(column));
                    }
                    else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void timestampColumn(final Column column)
                {
                    if (!singlePageRecordReader.isNull(column)) {
                        if (timestampFormatter == null) {
                            resultObject.put(column.getName(),
                                             singlePageRecordReader.getTimestamp(column).getEpochSecond());
                        }
                        else {
                            resultObject.put(column.getName(),
                                             timestampFormatter.format(singlePageRecordReader.getTimestamp(column)));
                        }
                    }
                    else if (fillsJsonNullForEmbulkNull) {
                        resultObject.putNull(column.getName());
                    }
                }

                @Override
                public void jsonColumn(final Column column)
                {
                    // TODO(dmikurube): Use jackson-datatype-msgpack.
                    // See: https://github.com/embulk/embulk-base-restclient/issues/32
                    if (!singlePageRecordReader.isNull(column)) {
                        String jsonText = singlePageRecordReader.getJson(column).toJson();
                        JsonNode node = jsonParser.parseJsonNode(jsonText);

                        if (node.isObject()) {
                            resultObject.set(column.getName(), jsonParser.parseJsonObject(jsonText));
                        }
                        else if (node.isArray()) {
                            resultObject.set(column.getName(), jsonParser.parseJsonArray(jsonText));
                        }
                        else {
                            throw new DataException("Unexpected node: " + jsonText);
                        }
                    }
                    else {
                        resultObject.putNull(column.getName());
                    }
                }
            });
        return resultObject;
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TimestampFormatter timestampFormatter;
    private final StringJsonParser jsonParser;
    private final boolean fillsJsonNullForEmbulkNull;
}
