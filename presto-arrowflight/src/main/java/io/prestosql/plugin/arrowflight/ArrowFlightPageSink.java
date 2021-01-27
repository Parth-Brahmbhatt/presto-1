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
