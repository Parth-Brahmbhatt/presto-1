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

import io.airlift.http.client.jetty.JettyHttpClient;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class NetflixEventListenerFactory
        implements EventListenerFactory
{
    private static final String EVENT_PUBLISH_URI = "event.publish.uri";

    private EventListener eventListener;

    private Object lock = new Object();

    @Override
    public String getName()
    {
        return "netflix-eventlistener";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        if (eventListener != null) {
            return eventListener;
        }

        synchronized (lock) {
            if (eventListener == null) {
                checkArgument(config != null && config.containsKey(EVENT_PUBLISH_URI), "No config found for " + EVENT_PUBLISH_URI);
                // TODO: ideally we want to use guice and inject JettyHttpClient with all of its configuration
                EventClient eventClient = new EventClient(new JettyHttpClient(), config.get(EVENT_PUBLISH_URI));
                this.eventListener = new NetflixEventListener(eventClient);
            }
        }
        return this.eventListener;
    }
}
