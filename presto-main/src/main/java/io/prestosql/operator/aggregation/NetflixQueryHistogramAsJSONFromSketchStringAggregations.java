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
import io.prestosql.operator.aggregation.state.NetflixHistogramState;
import io.prestosql.spi.PrestoException;
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
import static io.prestosql.operator.aggregation.NetflixQueryHistogramAsJSONFromSketchStringAggregations.NAME;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

@AggregationFunction(NAME)
public class NetflixQueryHistogramAsJSONFromSketchStringAggregations
{
    private NetflixQueryHistogramAsJSONFromSketchStringAggregations() {}

    public static final String NAME = "nf_query_histogram_json_sketch_string";

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.VARCHAR) Slice sketch)
    {
        try {
            addSketchString(state, sketch);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
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
            VARCHAR.writeString(out, digest.getJsonHistogram());
        }
    }
}
