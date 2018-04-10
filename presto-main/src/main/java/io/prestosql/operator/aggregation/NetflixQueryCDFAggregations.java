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
import io.airlift.slice.Slice;
import io.prestosql.operator.aggregation.state.NetflixQueryCDFArrayState;
import io.prestosql.operator.aggregation.state.NetflixQueryCDFState;
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

import java.util.List;

import static io.prestosql.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.addSketchString;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.combineStates;
import static io.prestosql.operator.aggregation.NetflixQueryCDFAggregations.NAME;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.util.Failures.checkCondition;

@AggregationFunction(NAME)
public class NetflixQueryCDFAggregations
{
    private NetflixQueryCDFAggregations() {}

    static final String NAME = "nf_query_cdf_from_sketch_string";

    @InputFunction
    public static void input(@AggregationState NetflixQueryCDFState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType(StandardTypes.DOUBLE) double cdf)
    {
        try {
            addSketchString(state, sketch);
            state.setCDF(cdf);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryCDFArrayState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType("array(double)") Block cdfsArrayBlock)
    {
        try {
            addSketchString(state, sketch);
            initializeCDFSArray(state, cdfsArrayBlock);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @CombineFunction
    public static void combine(NetflixQueryCDFState state, NetflixQueryCDFState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setCDF(otherState.getCDF());
    }

    @CombineFunction
    public static void combine(NetflixQueryCDFArrayState state, NetflixQueryCDFArrayState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setCDFS(otherState.getCDFS());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NetflixQueryCDFState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        double cdf = state.getCDF();
        if (digest == null || digest.getN() == 0) {
            out.appendNull();
        }
        else {
            double value = digest.cdf(cdf);
            DOUBLE.writeDouble(out, value);
        }
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState NetflixQueryCDFArrayState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        List<Double> cdfs = state.getCDFS();

        if (cdfs == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < cdfs.size(); i++) {
            Double cdf = cdfs.get(i);
            DOUBLE.writeDouble(blockBuilder, digest.cdf(cdf));
        }

        out.closeEntry();
    }

    private static void initializeCDFSArray(@AggregationState NetflixQueryCDFArrayState state, Block cdfsArrayBlock)
    {
        if (state.getCDFS() == null) {
            ImmutableList.Builder<Double> cdfsListBuilder = ImmutableList.builder();

            for (int i = 0; i < cdfsArrayBlock.getPositionCount(); i++) {
                checkCondition(!cdfsArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "cdf cannot be null");
                double cdf = DOUBLE.getDouble(cdfsArrayBlock, i);
                cdfsListBuilder.add(cdf);
            }
            state.setCDFS(cdfsListBuilder.build());
        }
    }
}
