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

public class NetflixQueryCDFArrayStateFactory
        implements AccumulatorStateFactory<NetflixQueryCDFArrayState>
{
    @Override
    public NetflixQueryCDFArrayState createSingleState()
    {
        return new NetflixSingleQueryCDFArrayState();
    }

    @Override
    public Class<? extends NetflixQueryCDFArrayState> getSingleStateClass()
    {
        return NetflixSingleQueryCDFArrayState.class;
    }

    @Override
    public NetflixQueryCDFArrayState createGroupedState()
    {
        return new NetflixGroupedQueryCDFArrayState();
    }

    @Override
    public Class<? extends NetflixQueryCDFArrayState> getGroupedStateClass()
    {
        return NetflixGroupedQueryCDFArrayState.class;
    }

    public static class NetflixGroupedQueryCDFArrayState
            extends AbstractGroupedAccumulatorState
            implements NetflixQueryCDFArrayState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixGroupedQueryCDFArrayState.class).instanceSize();
        private final ObjectBigArray<NetflixHistogram> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> cdfsArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            cdfsArray.ensureCapacity(size);
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
        public List<Double> getCDFS()
        {
            return cdfsArray.get(getGroupId());
        }

        @Override
        public void setCDFS(List<Double> cdfs)
        {
            cdfsArray.set(getGroupId(), requireNonNull(cdfs, "cdfs is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + cdfsArray.sizeOf();
        }
    }

    public static class NetflixSingleQueryCDFArrayState
            implements NetflixQueryCDFArrayState
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(NetflixSingleQueryCDFArrayState.class).instanceSize();
        private NetflixHistogram digest;
        private List<Double> cdfs;

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
        public List<Double> getCDFS()
        {
            return cdfs;
        }

        @Override
        public void setCDFS(List<Double> cdfs)
        {
            this.cdfs = requireNonNull(cdfs, "cdfs is null");
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
            if (cdfs != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(cdfs.size());
            }
            return estimatedSize;
        }
    }
}
