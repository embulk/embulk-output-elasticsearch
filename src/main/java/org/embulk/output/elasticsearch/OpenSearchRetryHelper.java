/*
 * Copyright 2020 The Embulk project
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

import jakarta.json.spi.JsonProvider;
import java.io.IOException;
import java.util.Locale;
import org.embulk.util.retryhelper.Retryable;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.slf4j.LoggerFactory;

public class OpenSearchRetryHelper
        implements AutoCloseable
{
    public OpenSearchRetryHelper(int maximumRetries,
                              int initialRetryIntervalMillis,
                              int maximumRetryIntervalMillis,
                              OpenSearchClientCreator clientCreator)
    {
        this.maximumRetries = maximumRetries;
        this.initialRetryIntervalMillis = initialRetryIntervalMillis;
        this.maximumRetryIntervalMillis = maximumRetryIntervalMillis;
        try {
            this.clientStarted = clientCreator.createAndStart();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.logger = LoggerFactory.getLogger(OpenSearchRetryHelper.class);
    }

    public <T> T requestWithRetry(final OpenSearchSingleRequester<T> singleRequester)
    {
        try {
            return RetryExecutor.builder()
                .withRetryLimit(this.maximumRetries)
                .withInitialRetryWaitMillis(this.initialRetryIntervalMillis)
                .withMaxRetryWaitMillis(this.maximumRetryIntervalMillis)
                .build()
                .runInterruptible(new Retryable<T>() {
                        @Override
                        public T call() throws Exception
                        {
                            try {
                                return singleRequester.requestOnce(clientStarted);
                            }
                            catch (OpenSearchException e) {
                                throw e;
                            }
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return singleRequester.toRetry(exception);
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format(
                                Locale.ENGLISH, "Retrying %d/%d after %d seconds. Message: %s",
                                retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception first, Exception last) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // InterruptedException must not be RuntimeException.
            throw new RuntimeException(ex);
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw new RuntimeException(ex.getCause());
        }
    }

    @Override
    public void close()
    {
        if (this.clientStarted == null) {
            return;
        }

        try {
            this.clientStarted._transport().close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public JsonpMapper jsonpMapper()
    {
        return this.clientStarted._transport().jsonpMapper();
    }

    public JsonProvider jsonProvider()
    {
        return jsonpMapper().jsonProvider();
    }

    private final int maximumRetries;
    private final int initialRetryIntervalMillis;
    private final int maximumRetryIntervalMillis;
    private final OpenSearchClient clientStarted;
    private final org.slf4j.Logger logger;
}
