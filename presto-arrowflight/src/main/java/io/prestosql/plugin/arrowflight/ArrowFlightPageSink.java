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
package io.prestosql.plugin.arrowflight;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class ArrowFlightPageSink
        implements ConnectorPageSink
{
    private ArrowFlightClient client;

    public ArrowFlightPageSink(ArrowFlightClient client)
    {
        this.client = client;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        client.send(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        client.finish();
        return CompletableFuture.completedFuture(Collections.EMPTY_LIST);
    }

    @Override
    public void abort()
    {
        //may be clean up
    }
}
