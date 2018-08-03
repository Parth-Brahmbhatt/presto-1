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

import io.airlift.log.Logger;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;

import javax.inject.Inject;

import java.io.IOException;

public class NetflixEventListener
        implements EventListener
{
    private EventClient eventClient;
    private final Logger log = Logger.get(NetflixEventListener.class);

    @Inject
    public NetflixEventListener(EventClient eventClient)
    {
        this.eventClient = eventClient;
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        try {
            this.eventClient.postEvent(queryCompletedEvent);
        }
        catch (IOException e) {
            log.warn("Failed to publish event " + queryCompletedEvent, e);
        }
    }
}
