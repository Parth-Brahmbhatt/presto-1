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

import com.netflix.data.datastructures.NetflixHistogram;
import io.airlift.slice.SizeOf;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NetflixQueryApproxPercentileArrayStateFactory
        implements AccumulatorStateFactory<NetflixQueryApproxPercentileArrayState>
{
    @Override
    public NetflixQueryApproxPercentileArrayState createSingleState()
    {
        return new NetflixSingleQueryApproxPercentileArrayState();
    }

    @Override
    public Class<? extends NetflixQueryApproxPercentileArrayState> getSingleStateClass()
    {
        return NetflixSingleQueryApproxPercentileArrayState.class;
    }

    @Override
    public NetflixQueryApproxPercentileArrayState createGroupedState()
    {
        return new NetflixGroupedQueryApproxPercentileArrayState();
    }

    @Override
    public Class<? extends NetflixQueryApproxPercentileArrayState> getGroupedStateClass()
    {
        return NetflixGroupedQueryApproxPercentileArrayState.class;
    }

    public static class NetflixGroupedQueryApproxPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements NetflixQueryApproxPercentileArrayState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixGroupedQueryApproxPercentileArrayState.class).instanceSize();
        private final ObjectBigArray<NetflixHistogram> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public NetflixHistogram getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(NetflixHistogram digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class NetflixSingleQueryApproxPercentileArrayState
            implements NetflixQueryApproxPercentileArrayState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixSingleQueryApproxPercentileArrayState.class).instanceSize();
        private NetflixHistogram digest;
        private List<Double> percentiles;

        @Override
        public NetflixHistogram getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(NetflixHistogram digest)
        {
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (digest != null) {
                estimatedSize += digest.getEstimatedMemorySize();
            }
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
