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
package io.prestosql.operator.aggregation.state;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.prestosql.operator.aggregation.NetflixHistogramUtils.deserializeHistogram;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class NetflixQueryCDFArrayStateSerializer
        implements AccumulatorStateSerializer<NetflixQueryCDFArrayState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(NetflixQueryCDFArrayState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            Slice digest = Slices.wrappedBuffer(state.getDigest().toBytes());

            SliceOutput output = Slices.allocate(
                    SIZE_OF_INT + // number of percentiles
                            state.getCDFS().size() * SIZE_OF_DOUBLE + // percentiles
                            SIZE_OF_INT + // digest length
                            digest.length()) // digest
                    .getOutput();

            // write percentiles
            List<Double> cdfs = state.getCDFS();
            output.appendInt(cdfs.size());
            for (double cdf : cdfs) {
                output.appendDouble(cdf);
            }

            output.appendInt(digest.length());
            output.appendBytes(digest);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, NetflixQueryCDFArrayState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read number of cdfs
        int numCdfs = input.readInt();

        ImmutableList.Builder<Double> cdfsListBuilder = ImmutableList.builder();
        for (int i = 0; i < numCdfs; i++) {
            cdfsListBuilder.add(input.readDouble());
        }
        state.setCDFS(cdfsListBuilder.build());

        // read digest
        int length = input.readInt();
        deserializeHistogram(state, input, length);
    }
}
