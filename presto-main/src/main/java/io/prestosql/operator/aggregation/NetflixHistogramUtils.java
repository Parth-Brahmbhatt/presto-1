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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import com.netflix.data.datastructures.NetflixHistogram;
import com.netflix.data.datastructures.NetflixHistogramException;
import com.netflix.data.datastructures.NetflixHistogramTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.prestosql.operator.aggregation.state.NetflixHistogramState;
import io.prestosql.operator.aggregation.state.NetflixQueryApproxPercentileArrayState;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.BigintOperators;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Math.toIntExact;

public class NetflixHistogramUtils
{
    public static final int TDIGEST_DEFAULT_COMPRESSION = 50;
    public static final int YAHOO_QUANTILE_SKETCH_DEFAULT_K = 128;

    private NetflixHistogramUtils() {}

    static void addValue(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        int w = checkWeight(weight);
        NetflixHistogramTypes type = getHistogramType((int) BigintOperators.castToInteger(histogramType));
        NetflixHistogram digest = initializeDigest(state, type, (int) BigintOperators.castToInteger(k));
        state.addMemoryUsage(-digest.getEstimatedMemorySize());
        digest.add(value, w);
        state.addMemoryUsage(digest.getEstimatedMemorySize());
    }

    static void addSketchString(@AggregationState NetflixHistogramState state, Slice sketch)
            throws NetflixHistogramException
    {
        if (!initializeDigestWithSketchString(state, sketch)) {
            NetflixHistogram digest = state.getDigest();
            state.addMemoryUsage(-digest.getEstimatedMemorySize());
            NetflixHistogram input = new NetflixHistogram(sketch.toStringUtf8());
            digest.add(input);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
        }
    }

    static void addSketchBytes(@AggregationState NetflixHistogramState state, Slice sketch)
            throws NetflixHistogramException
    {
        if (!initializeDigestWithSketchBytes(state, sketch)) {
            NetflixHistogram digest = state.getDigest();
            state.addMemoryUsage(-digest.getEstimatedMemorySize());
            NetflixHistogram input = new NetflixHistogram(sketch.getBytes());
            digest.add(input);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
        }
    }

    static NetflixHistogramTypes getHistogramType(int type)
            throws IllegalArgumentException
    {
        return Arrays.stream(NetflixHistogramTypes.values()).filter(e -> e.getValue() == type).findFirst().orElseThrow(() -> new IllegalArgumentException("Invalid histogram type " + type));
    }

    static void combineStates(NetflixHistogramState currentState, NetflixHistogramState otherState)
            throws NetflixHistogramException
    {
        NetflixHistogram otherDigest = otherState.getDigest();
        NetflixHistogram currentDigest = currentState.getDigest();
        if (currentDigest == null) {
            currentState.setDigest(otherDigest);
            currentState.addMemoryUsage(otherDigest.getEstimatedMemorySize());
        }
        else {
            currentState.addMemoryUsage(-currentDigest.getEstimatedMemorySize());
            currentDigest.add(otherDigest);
            currentState.addMemoryUsage(currentDigest.getEstimatedMemorySize());
        }
    }

    static int checkWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "percentile weight must be > 0");
        try {
            return toIntExact(weight);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Out of range for integer: " + weight, e);
        }
    }

    /**
     * @return true if the digest was initialized
     */
    static NetflixHistogram initializeDigest(@AggregationState NetflixHistogramState state, NetflixHistogramTypes histogramType, int k)
    {
        NetflixHistogram digest = state.getDigest();
        if (digest == null) {
            Properties properties = getHistogramProperties(histogramType, k);
            digest = new NetflixHistogram(histogramType, properties);
            state.setDigest(digest);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
        }
        return digest;
    }

    /**
     * @return true if the digest was initialized
     */
    static boolean initializeDigestWithSketchString(@AggregationState NetflixHistogramState state, Slice sketch)
            throws NetflixHistogramException
    {
        NetflixHistogram digest = state.getDigest();
        if (digest == null) {
            digest = new NetflixHistogram(sketch.toStringUtf8());
            state.setDigest(digest);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
            return true;
        }
        return false;
    }

    /**
     * @return true if the digest was initialized
     */
    static boolean initializeDigestWithSketchBytes(@AggregationState NetflixHistogramState state, Slice sketch)
            throws NetflixHistogramException
    {
        NetflixHistogram digest = state.getDigest();
        if (digest == null) {
            digest = new NetflixHistogram(sketch.getBytes());
            state.setDigest(digest);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
            return true;
        }
        return false;
    }

    private static Properties getHistogramProperties(NetflixHistogramTypes type, int k)
    {
        Properties props = new Properties();
        if (type == NetflixHistogramTypes.TDigest) {
            props.setProperty("compression", String.valueOf(k));
        }
        else {
            props.setProperty("k", String.valueOf(k));
        }
        return props;
    }

    static void writePercentileValue(BlockBuilder out, NetflixHistogram digest, double percentile)
    {
        if (digest == null || digest.getN() == 0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            double value = digest.quantile(percentile);
            DOUBLE.writeDouble(out, value);
        }
    }

    static void writePercentileValues(@AggregationState NetflixQueryApproxPercentileArrayState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            DOUBLE.writeDouble(blockBuilder, digest.quantile(percentile));
        }

        out.closeEntry();
    }

    static void initializePercentilesArray(@AggregationState NetflixQueryApproxPercentileArrayState state, Block percentilesArrayBlock)
    {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                percentilesListBuilder.add(percentile);
            }
            state.setPercentiles(percentilesListBuilder.build());
        }
    }

    public static void deserializeHistogram(NetflixHistogramState state, SliceInput input, int length)
    {
        try {
            NetflixHistogram digest = new NetflixHistogram(input.readSlice(length).getBytes());
            state.setDigest(digest);
            state.addMemoryUsage(digest.getEstimatedMemorySize());
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }
}
