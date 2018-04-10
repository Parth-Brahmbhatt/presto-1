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

import com.netflix.data.datastructures.NetflixHistogram;
import com.netflix.data.datastructures.NetflixHistogramException;
import io.airlift.slice.Slice;
import io.prestosql.operator.aggregation.state.NetflixQueryApproxPercentileArrayState;
import io.prestosql.operator.aggregation.state.NetflixQueryApproxPercentileState;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.addSketchString;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.combineStates;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.initializePercentilesArray;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.writePercentileValue;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.writePercentileValues;
import static io.prestosql.operator.aggregation.NetflixQueryApproxPercentileAggregations.NAME;

@AggregationFunction(NAME)
public class NetflixQueryApproxPercentileSketchStringAggregations
{
    private NetflixQueryApproxPercentileSketchStringAggregations() {}

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        try {
            addSketchString(state, sketch);
            state.setPercentile(percentile);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        try {
            addSketchString(state, sketch);
            initializePercentilesArray(state, percentilesArrayBlock);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixQueryApproxPercentileState state, NetflixQueryApproxPercentileState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setPercentile(otherState.getPercentile());
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixQueryApproxPercentileArrayState state, NetflixQueryApproxPercentileArrayState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NetflixQueryApproxPercentileState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        double percentile = state.getPercentile();
        writePercentileValue(out, digest, percentile);
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState NetflixQueryApproxPercentileArrayState state, BlockBuilder out)
    {
        writePercentileValues(state, out);
    }
}
