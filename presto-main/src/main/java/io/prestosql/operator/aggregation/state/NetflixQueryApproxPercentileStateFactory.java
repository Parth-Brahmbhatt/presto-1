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
import io.prestosql.array.DoubleBigArray;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class NetflixQueryApproxPercentileStateFactory
        implements AccumulatorStateFactory<NetflixQueryApproxPercentileState>
{
    @Override
    public NetflixQueryApproxPercentileState createSingleState()
    {
        return new NetflixSingleQueryApproxPercentileState();
    }

    @Override
    public Class<? extends NetflixQueryApproxPercentileState> getSingleStateClass()
    {
        return NetflixSingleQueryApproxPercentileState.class;
    }

    @Override
    public NetflixQueryApproxPercentileState createGroupedState()
    {
        return new NetflixGroupedQueryApproxPercentileState();
    }

    @Override
    public Class<? extends NetflixQueryApproxPercentileState> getGroupedStateClass()
    {
        return NetflixGroupedQueryApproxPercentileState.class;
    }

    public static class NetflixGroupedQueryApproxPercentileState
            extends AbstractGroupedAccumulatorState
            implements NetflixQueryApproxPercentileState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixGroupedQueryApproxPercentileState.class).instanceSize();
        private final ObjectBigArray<NetflixHistogram> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public NetflixHistogram getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(NetflixHistogram digest)
        {
            requireNonNull(digest, "value is null");
            digests.set(getGroupId(), digest);
        }

        @Override
        public double getPercentile()
        {
            return percentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double percentile)
        {
            percentiles.set(getGroupId(), percentile);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentiles.sizeOf();
        }
    }

    public static class NetflixSingleQueryApproxPercentileState
            implements NetflixQueryApproxPercentileState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixSingleQueryApproxPercentileState.class).instanceSize();
        private NetflixHistogram digest;
        private double percentile;

        @Override
        public NetflixHistogram getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(NetflixHistogram digest)
        {
            this.digest = digest;
        }

        @Override
        public double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
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
            return estimatedSize;
        }
    }
}
