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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Math.toIntExact;
import static java.net.InetAddress.getLocalHost;
import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

public class ArrowFlightClient
{
    private List<ArrowColumnHandle> columns;
    private FlightClient client;
    private BufferAllocator allocator;
    private Schema schema;
    private VectorSchemaRoot root;
    private SchemaTableName tableName;
    private final FlightClient.ClientStreamListener listener;

    public ArrowFlightClient(SchemaTableName tableName, List<ArrowColumnHandle> columns)
    {
        this.tableName = tableName;
        final String arrowServer = tableName.getSchemaName();
        final String[] serverAndPort = arrowServer.split(":");
        this.allocator = new RootAllocator(Integer.MAX_VALUE);

        this.client = FlightClient.builder(allocator, Location.forGrpcInsecure(serverAndPort[0], Integer.parseInt(serverAndPort[1]))).build();
        this.columns = columns;
        final List<Field> fields = columns.stream()
                .map(this::toArrowField)
                .collect(Collectors.toList());
        this.schema = new Schema(fields);
        this.root = VectorSchemaRoot.create(schema, allocator);
        final FlightDescriptor flightDescriptor;
        try {
            flightDescriptor = FlightDescriptor.path(tableName.getTableName() + getLocalHost().getCanonicalHostName());
        }
        catch (UnknownHostException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        this.listener = client.startPut(flightDescriptor, root, new AsyncPutListener());
        //TODO check if a stream already exists with same name on server, or do that check through getTable
        // but even than we probably have to check it here depending on server implementation
    }

    public void send(Page page)
    {
        final int columnCount = page.getChannelCount();
        final int rowCount = page.getPositionCount();
        root.allocateNew();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final FieldVector vector = this.root.getVector(columnIndex);

            for (int rowNumber = 0; rowNumber < rowCount; rowNumber++) {
                final Type type = columns.get(columnIndex).getType();
                final Object value = TypeUtils.readNativeValue(type, page.getBlock(columnIndex), rowNumber);

                if (value == null) {
                    try {
                        vector.getClass().getMethod("setNull", int.class).invoke(vector, rowNumber);
                    }
                    catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
                    }
                }

                else if (type instanceof IntegerType || type instanceof BigintType) {
                    ((BaseIntVector) vector).setWithPossibleTruncate(rowNumber, (Long) value);
                }

                else if (type instanceof RealType) {
                    ((FloatingPointVector) vector).setSafeWithPossibleTruncate(rowNumber, longBitsToDouble(toIntExact((long) value)));
                }

                else if (type instanceof VarcharType) {
                    ((VarCharVector) vector).setSafe(rowNumber, new Text(((Slice) value).toStringUtf8()));
                }
                else {
                    throw new UnsupportedOperationException("vector type = " + vector.getClass() + " and presto type = " + type.getDisplayName() + " not supported");
                }
            }
            vector.setValueCount(rowCount);
        }
        root.setRowCount(rowCount);
        listener.putNext();
    }

    public void finish()
    {
        root.clear();
        listener.completed();
        listener.getResult();
        // wait for ack to avoid memory leaks.
        // Uncomment to actually see how many rows were pushed.
//        FlightInfo info = client.getInfo(FlightDescriptor.path(tableName.getTableName()));
//        try (final FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
//            while (stream.next()) {
//                VectorSchemaRoot newRoot = stream.getRoot();
//                System.out.println(newRoot.getRowCount());
//            }
//        }
//        catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    private FieldVector toArrowVector(ArrowColumnHandle columnHandle)
    {
        final BufferAllocator bufferAllocator = this.allocator.newChildAllocator(columnHandle.getColumnName(), 0, Long.MAX_VALUE);
        final Type type = columnHandle.getType();
        if (type instanceof BooleanType) {
            return new BitVector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof IntegerType) {
            return new IntVector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof BigintType) {
            return new BigIntVector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof RealType) {
            return new Float4Vector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof DoubleType) {
            return new Float8Vector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = ((DecimalType) type);
            return new DecimalVector(columnHandle.getColumnName(), bufferAllocator, decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof VarcharType) {
            return new VarCharVector(columnHandle.getColumnName(), bufferAllocator);
        }
        if (type instanceof VarbinaryType) {
            return new VarBinaryVector(columnHandle.getColumnName(), bufferAllocator);
        }
//        if (type instanceof DateType) {
//            return new Date(columnHandle.getColumnName(), bufferAllocator);
//        }
//        if (type.equals(TIME_MICROS)) {
//            return Types.TimeType.get();
//        }
//        if (type.equals(TIMESTAMP_MICROS)) {
//            return Types.TimestampType.withoutZone();
//        }
//        if (type.equals(TIMESTAMP_TZ_MICROS)) {
//            return Types.TimestampType.withZone();
//        }
//        if (type instanceof RowType) {
//            return fromRow((RowType) type);
//        }
//        if (type instanceof ArrayType) {
//            return fromArray((ArrayType) type);
//        }
//        if (type instanceof MapType) {
//            return fromMap((MapType) type);
//        }
//        if (type instanceof TimeType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Time precision (%s) not supported for Iceberg. Use \"time(6)\" instead.", ((TimeType) type).getPrecision()));
//        }
//        if (type instanceof TimestampType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6)\" instead.", ((TimestampType) type).getPrecision()));
//        }
//        if (type instanceof TimestampWithTimeZoneType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6) with time zone\" instead.", ((TimestampWithTimeZoneType) type).getPrecision()));
//        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }

    private Field toArrowField(ArrowColumnHandle columnHandle)
    {
        final Type type = columnHandle.getType();
        if (type instanceof BooleanType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), Binary.INSTANCE);
        }
        if (type instanceof IntegerType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), new ArrowType.Int(8, true));
        }
        if (type instanceof BigintType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), new ArrowType.Int(64, true));
        }
        if (type instanceof RealType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), new ArrowType.FloatingPoint(SINGLE));
        }
        if (type instanceof DoubleType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), new ArrowType.FloatingPoint(DOUBLE));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = ((DecimalType) type);
            return Field.nullablePrimitive(columnHandle.getColumnName(), new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128));
        }
        if (type instanceof VarcharType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), ArrowType.Utf8.INSTANCE);
        }
        if (type instanceof VarbinaryType) {
            return Field.nullablePrimitive(columnHandle.getColumnName(), ArrowType.Binary.INSTANCE);
        }
//        if (type instanceof DateType) {
//            return new Date(columnHandle.getColumnName(), bufferAllocator);
//        }
//        if (type.equals(TIME_MICROS)) {
//            return Types.TimeType.get();
//        }
//        if (type.equals(TIMESTAMP_MICROS)) {
//            return Types.TimestampType.withoutZone();
//        }
//        if (type.equals(TIMESTAMP_TZ_MICROS)) {
//            return Types.TimestampType.withZone();
//        }
//        if (type instanceof RowType) {
//            return fromRow((RowType) type);
//        }
//        if (type instanceof ArrayType) {
//            return fromArray((ArrayType) type);
//        }
//        if (type instanceof MapType) {
//            return fromMap((MapType) type);
//        }
//        if (type instanceof TimeType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Time precision (%s) not supported for Iceberg. Use \"time(6)\" instead.", ((TimeType) type).getPrecision()));
//        }
//        if (type instanceof TimestampType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6)\" instead.", ((TimestampType) type).getPrecision()));
//        }
//        if (type instanceof TimestampWithTimeZoneType) {
//            throw new PrestoException(NOT_SUPPORTED, format("Timestamp precision (%s) not supported for Iceberg. Use \"timestamp(6) with time zone\" instead.", ((TimestampWithTimeZoneType) type).getPrecision()));
//        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported for Iceberg: " + type.getDisplayName());
    }
}
