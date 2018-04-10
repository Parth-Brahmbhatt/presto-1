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

public class NetflixQueryCDFStateFactory
        implements AccumulatorStateFactory<NetflixQueryCDFState>
{
    @Override
    public NetflixQueryCDFState createSingleState()
    {
        return new NetflixSingleQueryCDFState();
    }

    @Override
    public Class<? extends NetflixQueryCDFState> getSingleStateClass()
    {
        return NetflixSingleQueryCDFState.class;
    }

    @Override
    public NetflixQueryCDFState createGroupedState()
    {
        return new NetflixGroupedQueryCDFState();
    }

    @Override
    public Class<? extends NetflixQueryCDFState> getGroupedStateClass()
    {
        return NetflixGroupedQueryCDFState.class;
    }

    public static class NetflixGroupedQueryCDFState
            extends AbstractGroupedAccumulatorState
            implements NetflixQueryCDFState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixGroupedQueryCDFState.class).instanceSize();
        private final ObjectBigArray<NetflixHistogram> digests = new ObjectBigArray<>();
        private final DoubleBigArray cdfs = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            cdfs.ensureCapacity(size);
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
        public double getCDF()
        {
            return cdfs.get(getGroupId());
        }

        @Override
        public void setCDF(double cdf)
        {
            cdfs.set(getGroupId(), cdf);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + cdfs.sizeOf();
        }
    }

    public static class NetflixSingleQueryCDFState
            implements NetflixQueryCDFState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixSingleQueryCDFState.class).instanceSize();
        private NetflixHistogram digest;
        private double cdf;

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
        public double getCDF()
        {
            return cdf;
        }

        @Override
        public void setCDF(double cdf)
        {
            this.cdf = cdf;
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
