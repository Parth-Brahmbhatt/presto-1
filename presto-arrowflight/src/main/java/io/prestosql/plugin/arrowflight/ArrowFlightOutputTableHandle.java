package io.prestosql.plugin.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;

public class ArrowFlightOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private SchemaTableName tableName;
    private List<ArrowColumnHandle> columns;

    @JsonCreator
    public ArrowFlightOutputTableHandle(
            @JsonProperty("table") SchemaTableName table,
            @JsonProperty("columns") List<ArrowColumnHandle> columns)
    {
        this.tableName = table;
        this.columns = columns;
    }

    @JsonProperty
    public SchemaTableName getTable()
    {
        return tableName;
    }

    @JsonProperty
    public List<ArrowColumnHandle> getColumns()
    {
        return columns;
    }
}
