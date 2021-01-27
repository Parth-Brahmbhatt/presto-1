package io.prestosql.plugin.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.Type;

import java.util.Objects;
import java.util.StringJoiner;

public class ArrowColumnHandle
{
    private String columnName;
    private Type type;
    private boolean nullable;

    @JsonCreator
    public ArrowColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("type") Type type,
            @JsonProperty("nullable") boolean nullable)
    {
        this.columnName = columnName;
        this.type = type;
        this.nullable = nullable;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ArrowColumnHandle that = (ArrowColumnHandle) o;
        return nullable == that.nullable &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, type, nullable);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", ArrowColumnHandle.class.getSimpleName() + "[", "]")
                .add("columnName='" + columnName + "'")
                .add("type=" + type)
                .add("nullable=" + nullable)
                .toString();
    }
}
