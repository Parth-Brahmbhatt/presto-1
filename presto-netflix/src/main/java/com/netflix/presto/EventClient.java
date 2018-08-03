/*
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
package com.netflix.presto;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.airlift.event.client.AbstractEventClient;
import io.airlift.event.client.JsonEventSerializer;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EventClient
        extends AbstractEventClient
{
    private static final int MAX_CLIENT_THREADS = 4; //TODO hardcoded for now
    private final Logger log = Logger.get(EventClient.class);
    private static final int MAX_RETRIES = 5;
    private static final Duration MAX_RETRY_TIME = new Duration(5, SECONDS);

    private ExecutorService executor;
    private HttpClient httpClient;
    private final JsonFactory factory = new JsonFactory();
    private final URI eventPublishUri;

    @Inject
    public EventClient(HttpClient httpClient, String eventPublishUri)
    {
        ExecutorService coreExecutor = newCachedThreadPool(daemonThreadsNamed("netflix-event-client-%s"));
        this.httpClient = httpClient;
        this.executor = ExecutorServiceAdapter.from(new BoundedExecutor(coreExecutor, MAX_CLIENT_THREADS));
        this.eventPublishUri = URI.create(eventPublishUri);
    }

    @Override
    protected <T> void postEvent(T event)
            throws IOException
    {
        if (eventPublishUri == null) {
            //discard event
            return;
        }

        //for now only hook into the query completion events
        if (event instanceof QueryCompletedEvent) {
            final QueryCompletionEvent queryCompletionEvent = QueryCompletionEvent.fromQueryCompletedEvent((QueryCompletedEvent) event);
            AtomicInteger retries = new AtomicInteger();
            final long start = System.nanoTime();
            final Request request = preparePost()
                    .setBodyGenerator(new EventJsonGenerator(queryCompletionEvent, factory))
                    .setUri(eventPublishUri)
                    .build();

            Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), new FutureCallback<StatusResponseHandler.StatusResponse>()
            {
                @Override
                public void onSuccess(StatusResponseHandler.StatusResponse result)
                {
                    // assume any response is good enough
                }

                @Override
                public void onFailure(Throwable t)
                {
                    if (t instanceof RejectedExecutionException) {
                        // client has been shutdown
                        log.error("Probably client has been shutdown", t);
                        return;
                    }

                    // reschedule with backoff
                    if (retries.intValue() <= MAX_RETRIES && nanosSince(start).compareTo(MAX_RETRY_TIME) < 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep((int) (200 * Math.pow(2, retries.getAndIncrement())));
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw Throwables.propagate(e);
                        }
                        Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), this, executor);
                    }
                    else {
                        log.warn("Unable to publish event after %d retries: %s", MAX_RETRIES, t.getMessage());
                    }
                }
            }, executor);
        }
    }

    class EventJsonGenerator
            implements BodyGenerator
    {
        //TODO JsonEventSerializer supports creating a serializer for multiple event classes
        //we can use it when we listen to other event types
        private final JsonEventSerializer serializer;
        private final JsonFactory factory;
        private final Object event;

        public EventJsonGenerator(Object event, JsonFactory factory)
        {
            this.event = event;
            this.factory = factory;
            this.serializer = new JsonEventSerializer(event.getClass());
        }

        @Override
        public void write(OutputStream out)
                throws Exception
        {
            JsonGenerator generator = factory.createGenerator(out, JsonEncoding.UTF8);
            serializer.serialize(event, generator);
        }
    }
}
