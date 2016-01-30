package org.embulk.output.elasticsearch.v2;

import com.google.common.base.Throwables;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.embulk.output.elasticsearch.ElasticsearchOutputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ElasticsearchBulkProcessor
        implements org.embulk.output.elasticsearch.ElasticsearchBulkProcessor
{
    public static class Builder
            implements org.embulk.output.elasticsearch.ElasticsearchBulkProcessor.Builder
    {
        private Logger log;
        private ElasticsearchClient client;

        @Override
        public Builder setLogger(Logger log)
        {
            this.log = log;
            return this;
        }

        @Override
        public Builder setClient(org.embulk.output.elasticsearch.ElasticsearchClient client)
        {
            this.client = (ElasticsearchClient) client;
            return this;
        }

        @Override
        public org.embulk.output.elasticsearch.ElasticsearchBulkProcessor build(PluginTask task)
        {
            BulkProcessor bulkProcessor = BulkProcessor.builder(client.getClient(), new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request)
                {
                    log.info("Execute {} bulk actions", request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
                {
                    if (response.hasFailures()) {
                        long items = 0;
                        if (log.isDebugEnabled()) {
                            for (BulkItemResponse item : response.getItems()) {
                                if (item.isFailed()) {
                                    items += 1;
                                    log.debug("   Error for {}/{}/{} for {} operation: {}",
                                            item.getIndex(), item.getType(), item.getId(),
                                            item.getOpType(), item.getFailureMessage());
                                }
                            }
                        }
                        log.warn("{} bulk actions failed: {}", items, response.buildFailureMessage());
                    } else {
                        log.info("{} bulk actions succeeded", request.numberOfActions());
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure)
                {
                    log.warn("Got the error during bulk processing", failure);
                }
            }).setBulkActions(task.getBulkActions())
                    .setBulkSize(new ByteSizeValue(task.getBulkSize()))
                    .setConcurrentRequests(task.getConcurrentRequests())
                    .build();
            return new ElasticsearchBulkProcessor(log, bulkProcessor, task.getIndex(), task.getType());
        }
    }

    private final Logger log;
    private final BulkProcessor bulkProcessor;

    private final String index;
    private final String type;

    ElasticsearchBulkProcessor(Logger log, BulkProcessor bulkProcessor, String index, String type)
    {
        this.log = log;
        this.bulkProcessor = bulkProcessor;

        this.index = index;
        this.type = type;
    }

    public void addIndexRequest(final PageReader pageReader, Column idColumn)
            throws IOException
    {
        final XContentBuilder contextBuilder = XContentFactory.jsonBuilder().startObject(); //  TODO reusable??
        pageReader.getSchema().visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column) {
                try {
                    if (pageReader.isNull(column)) {
                        contextBuilder.nullField(column.getName());
                    } else {
                        contextBuilder.field(column.getName(), pageReader.getBoolean(column));
                    }
                } catch (IOException e) {
                    try {
                        contextBuilder.nullField(column.getName());
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            @Override
            public void longColumn(Column column) {
                try {
                    if (pageReader.isNull(column)) {
                        contextBuilder.nullField(column.getName());
                    } else {
                        contextBuilder.field(column.getName(), pageReader.getLong(column));
                    }
                } catch (IOException e) {
                    try {
                        contextBuilder.nullField(column.getName());
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            @Override
            public void doubleColumn(Column column) {
                try {
                    if (pageReader.isNull(column)) {
                        contextBuilder.nullField(column.getName());
                    } else {
                        contextBuilder.field(column.getName(), pageReader.getDouble(column));
                    }
                } catch (IOException e) {
                    try {
                        contextBuilder.nullField(column.getName());
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            @Override
            public void stringColumn(Column column) {
                try {
                    if (pageReader.isNull(column)) {
                        contextBuilder.nullField(column.getName());
                    } else {
                        contextBuilder.field(column.getName(), pageReader.getString(column));
                    }
                } catch (IOException e) {
                    try {
                        contextBuilder.nullField(column.getName());
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            @Override
            public void timestampColumn(Column column) {
                try {
                    if (pageReader.isNull(column)) {
                        contextBuilder.nullField(column.getName());
                    } else {
                        contextBuilder.field(column.getName(), new Date(pageReader.getTimestamp(column).toEpochMilli()));
                    }
                } catch (IOException e) {
                    try {
                        contextBuilder.nullField(column.getName());
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }
        });

        contextBuilder.endObject();
        bulkProcessor.add(newIndexRequest(getIdValue(pageReader, idColumn)).source(contextBuilder));
    }

    private IndexRequest newIndexRequest(String idValue)
    {
        return Requests.indexRequest(index).type(type).id(idValue);
    }

    private String getIdValue(PageReader pageReader, Column inputColumn) {
        if (inputColumn == null) return null;
        if (pageReader.isNull(inputColumn)) return null;
        String idValue = null;
        if (Types.STRING.equals(inputColumn.getType())) {
            idValue = pageReader.getString(inputColumn);
        } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
            idValue = pageReader.getBoolean(inputColumn) + "";
        } else if (Types.DOUBLE.equals(inputColumn.getType())) {
            idValue = pageReader.getDouble(inputColumn) + "";
        } else if (Types.LONG.equals(inputColumn.getType())) {
            idValue = pageReader.getLong(inputColumn) + "";
        } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
            idValue = pageReader.getTimestamp(inputColumn).toString();
        } else {
            idValue = null;
        }
        return idValue;
    }

    @Override
    public void flush()
    {
        bulkProcessor.flush();
    }

    @Override
    public boolean awaitClose(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return bulkProcessor.awaitClose(timeout, unit);
    }

    @Override
    public void close()
            throws IOException
    {
        bulkProcessor.close();
    }
}
