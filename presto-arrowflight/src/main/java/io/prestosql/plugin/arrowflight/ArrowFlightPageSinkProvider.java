package io.prestosql.plugin.arrowflight;

import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

public class ArrowFlightPageSinkProvider
        implements ConnectorPageSinkProvider
{
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        final ArrowFlightOutputTableHandle handle = (ArrowFlightOutputTableHandle) outputTableHandle;
        ArrowFlightClient client = new ArrowFlightClient(handle.getTable(), handle.getColumns());
        return new ArrowFlightPageSink(client);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        throw new UnsupportedOperationException("Use Create table for now");
    }
}
