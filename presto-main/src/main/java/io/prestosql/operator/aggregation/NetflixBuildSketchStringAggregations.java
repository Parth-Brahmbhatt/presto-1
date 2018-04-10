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
import com.netflix.data.datastructures.NetflixHistogramTypes;
import io.prestosql.operator.aggregation.state.NetflixHistogramState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.NetflixBuildSketchStringAggregations.NAME;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.TDIGEST_DEFAULT_COMPRESSION;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.YAHOO_QUANTILE_SKETCH_DEFAULT_K;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.addValue;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.combineStates;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.getHistogramType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

@AggregationFunction(NAME)
public class NetflixBuildSketchStringAggregations
{
    private NetflixBuildSketchStringAggregations() {}

    public static final String NAME = "nf_build_sketch_string";

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, value, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        input(state, value, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        addValue(state, value, 1, histogramType, k);
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixHistogramState state, NetflixHistogramState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(@AggregationState NetflixHistogramState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        if (digest == null) {
            out.appendNull();
        }
        else {
            VARCHAR.writeString(out, digest.toBase64String());
        }
    }
}
