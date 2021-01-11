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
import io.airlift.slice.Slices;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.operator.aggregation.TestApproximatePercentileAggregation.createRLEBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestNetflixHistogramAggregation
        extends AbstractTestFunctions
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(DOUBLE, DOUBLE)
                    )
            );

    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(DOUBLE, DOUBLE, BIGINT)
                    )
            );

    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(DOUBLE, DOUBLE, BIGINT, BIGINT)
                    )
            );

    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                    TypeSignatureProvider.fromTypes(DOUBLE, new ArrayType(DOUBLE))
            ));

    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE =
            metadata.getAggregateFunctionImplementation(metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                    TypeSignatureProvider.fromTypes(DOUBLE, new ArrayType(DOUBLE), BIGINT)
            ));

    private static final InternalAggregationFunction
            NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION =
            metadata.getAggregateFunctionImplementation(metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                    TypeSignatureProvider.fromTypes(DOUBLE, new ArrayType(DOUBLE), BIGINT, BIGINT)
            ));

    //weighted percentile
    private static final InternalAggregationFunction NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(DOUBLE, BIGINT, DOUBLE)
                    )
            );

    //build sketch functions
    private static final InternalAggregationFunction NETFLIX_BUILD_SKETCH_STRING_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixBuildSketchStringAggregations.NAME),
                            TypeSignatureProvider.fromTypes(DOUBLE)
                    ));

    private static final InternalAggregationFunction NETFLIX_BUILD_SKETCH_BINARY_WITH_DEFAULTS = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(
                    QualifiedName.of(NetflixBuildSketchBytesAggregations.NAME),
                    TypeSignatureProvider.fromTypes(DOUBLE)
            ));

    // combine sketch functions
    private static final InternalAggregationFunction NETFLIX_COMBINE_SKETCH_STRINGS_WITH_DEFAULTS = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(
                    QualifiedName.of(NetflixCombineSketchBytesAggregations.NAME),
                    TypeSignatureProvider.fromTypes(VARCHAR)
            ));
    private static final InternalAggregationFunction NETFLIX_COMBINE_SKETCH_BYTES_WITH_DEFAULTS = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(
                    QualifiedName.of(NetflixCombineSketchBytesAggregations.NAME),
                    TypeSignatureProvider.fromTypes(VARBINARY)
            ));

    // query cdf functions
    private static final InternalAggregationFunction NETFLIX_QUERY_CDF_FROM_SKETCH_STRINGS_WITH_DEFAULTS = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryCDFAggregations.NAME),
                    TypeSignatureProvider.fromTypes(VARCHAR, DOUBLE)
            ));

    private static final InternalAggregationFunction NETFLIX_QUERY_CDF_ARRAY_FROM_SKETCH_STRINGS_WITH_DEFAULTS = metadata
            .getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryCDFAggregations.NAME),
                            TypeSignatureProvider.fromTypes(VARCHAR, new ArrayType(DOUBLE))
                    ));

    // query percentile from sketch functions
    private static final InternalAggregationFunction NETFLIX_QUERY_PERCENTILE_FROM_SKETCH_STRINGS_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                    TypeSignatureProvider.fromTypes(VARCHAR, DOUBLE)
            ));

    private static final InternalAggregationFunction NETFLIX_QUERY_PERCENTILE_ARRAY_FROM_SKETCH_STRINGS_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(metadata.resolveFunction(
                    QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                    TypeSignatureProvider.fromTypes(VARCHAR, new ArrayType(DOUBLE))
            ));

    private static final InternalAggregationFunction NETFLIX_QUERY_PERCENTILE_SKETCH_BYTES_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(VARBINARY, DOUBLE)
                    ));

    private static final InternalAggregationFunction NETFLIX_QUERY_PERCENTILE_ARRAY_SKETCH_BYTES_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryApproxPercentileAggregations.NAME),
                            TypeSignatureProvider.fromTypes(VARBINARY, new ArrayType(DOUBLE))
                    ));

    private static final InternalAggregationFunction NETFLIX_QUERY_JSON_HISTOGRAM_SKETCH_STRINGS_WITH_DEFAULTS =
            metadata.getAggregateFunctionImplementation(
                    metadata.resolveFunction(
                            QualifiedName.of(NetflixQueryHistogramAsJSONFromSketchStringAggregations.NAME),
                            TypeSignatureProvider.fromTypes(VARCHAR)
                    ));

    @Test
    public void testApproxPercentile()
    {
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.5, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.5, 2),
                createRLEBlock(1, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.5, 2),
                createRLEBlock(1, 2),
                createRLEBlock(64, 2)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS,
                1.0D,
                createDoublesBlock(null, 1.0D),
                createRLEBlock(0.5D, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE,
                1.0,
                createDoublesBlock(null, 1.0),
                createRLEBlock(0.5, 2),
                createRLEBlock(1, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION,
                1.0,
                createDoublesBlock(null, 1.0),
                createRLEBlock(0.5, 2),
                createRLEBlock(1, 2),
                createRLEBlock(64, 2)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(0.5, 4)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(0.5, 4),
                createRLEBlock(1, 4)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(0.5, 4),
                createRLEBlock(1, 4),
                createRLEBlock(64, 4)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(0.5, 3)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(0.5, 3),
                createRLEBlock(1, 3)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(0.5, 3),
                createRLEBlock(1, 3),
                createRLEBlock(64, 3)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_DEFAULTS,
                3.0,
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(0.5, 21)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE,
                3.0,
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(0.5, 21),
                createRLEBlock(1, 21)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_AGGREGATION_WITH_TYPE_COMPRESSION,
                3.0,
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(0.5, 21),
                createRLEBlock(1, 21),
                createRLEBlock(64, 21)
        );

        //array of approx_percentile
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2),
                createRLEBlock(1, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2),
                createRLEBlock(1, 2),
                createRLEBlock(64, 2)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2),
                createRLEBlock(1, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2),
                createRLEBlock(1, 2),
                createRLEBlock(64, 2)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                ImmutableList.of(1.0, 1.0),
                createDoublesBlock(null, 1.0),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                ImmutableList.of(1.0, 1.0),
                createDoublesBlock(null, 1.0),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2),
                createRLEBlock(1, 2)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                ImmutableList.of(1.0, 1.0),
                createDoublesBlock(null, 1.0),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2),
                createRLEBlock(1, 2),
                createRLEBlock(64, 2)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                ImmutableList.of(1.1, 2.0, 2.9000000000000004),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                ImmutableList.of(1.1, 2.0, 2.9000000000000004),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4),
                createRLEBlock(1, 4)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                ImmutableList.of(1.1, 2.0, 2.9000000000000004),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4),
                createRLEBlock(1, 4),
                createRLEBlock(64, 4)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3),
                createRLEBlock(1, 3)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3),
                createRLEBlock(1, 3),
                createRLEBlock(64, 3)
        );

        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_DEFAULTS,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21),
                createRLEBlock(1, 21)
        );
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21),
                createRLEBlock(1, 21),
                createRLEBlock(64, 21)
        );
        // try with yahoo quantile sketches
        assertAggregation(
                NETFLIX_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION_WITH_TYPE_COMPRESSION,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(
                        1.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        2.0,
                        2.0,
                        null,
                        3.0,
                        3.0,
                        null,
                        3.0,
                        null,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        7.0
                ),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21),
                createRLEBlock(2, 21),
                createRLEBlock(64, 21)
        );
    }

    // @Test ignoring the test for now till we have a solution for https://github.com/tdunning/t-digest/issues/114
    // @Test(enabled = false)
    // public void testApproxWeightedPercentile()
    // {
    //     assertAggregation(
    //             NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS,
    //             null,
    //             createDoublesBlock(null, null),
    //             createLongsBlock(1L, 1L),
    //             createRLEBlock(0.5, 2));
    //
    //     assertAggregation(
    //             NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS,
    //             1.0,
    //             createDoublesBlock(null, 1.0),
    //             createLongsBlock(1L, 1L),
    //             createRLEBlock(0.5, 2));
    //
    //     assertAggregation(
    //             NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS,
    //             2.0,
    //             createDoublesBlock(null, 1.0, 2.0, 3.0),
    //             createLongsBlock(1L, 1L, 1L, 1L),
    //             createRLEBlock(0.5, 4));
    //
    //     assertAggregation(
    //             NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS,
    //             2.0,
    //             createDoublesBlock(1.0, 2.0, 3.0),
    //             createLongsBlock(1L, 1L, 1L),
    //             createRLEBlock(0.5, 3));
    //
    //     assertAggregation(
    //             NETFLIX_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_DEFAULTS,
    //             2.75,
    //             createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
    //             createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
    //             createRLEBlock(0.5, 17));
    // }
    //

    @Test
    public void testSketchString()
    {
        NetflixHistogram hist = new NetflixHistogram();
        hist.add(1.0);
        hist.add(2.0);
        hist.add(2.0);
        hist.add(2.0);
        hist.add(2.0);
        hist.add(3.0);
        hist.add(3.0);
        hist.add(4.0);
        hist.add(5.0);
        hist.add(6.0);
        hist.add(7.0);
        String expectedSketchString = hist.toBase64String();
        assertAggregation(
                NETFLIX_BUILD_SKETCH_STRING_WITH_DEFAULTS,
                expectedSketchString,
                createDoublesBlock(1.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0)
        );
    }

    @Test
    public void testSketchBinaryRepresentation()
    {
        NetflixHistogram hist = new NetflixHistogram();
        hist.add(1.0);
        hist.add(2.0);
        hist.add(2.0);
        SqlVarbinary expectedValue = new SqlVarbinary(hist.toBytes().array());
        assertAggregation(
                NETFLIX_BUILD_SKETCH_BINARY_WITH_DEFAULTS,
                expectedValue,
                createDoublesBlock(1.0, 2.0, 2.0)
        );
    }

    @Test
    public void testCombiningSketchStrings()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        String sketch1 = hist1.toBase64String();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        String sketch2 = hist2.toBase64String();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        String sketch3 = hist3.toBase64String();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));

        String combinedSketch = combinedHist.toBase64String();
        assertAggregation(
                NETFLIX_COMBINE_SKETCH_STRINGS_WITH_DEFAULTS,
                combinedSketch,
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3))
        );
    }

    @Test
    public void testCombiningSketchBytes()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        ByteBuffer sketch1 = hist1.toBytes();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        ByteBuffer sketch2 = hist2.toBytes();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        ByteBuffer sketch3 = hist3.toBytes();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));

        SqlVarbinary combinedSketch = new SqlVarbinary(combinedHist.toBytes().array());
        assertAggregation(
                NETFLIX_COMBINE_SKETCH_BYTES_WITH_DEFAULTS,
                combinedSketch,
                createSlicesBlock(ImmutableList.of(
                        Slices.wrappedBuffer(sketch1),
                        Slices.wrappedBuffer(sketch2),
                        Slices.wrappedBuffer(sketch3)
                ))
        );
    }

    @Test
    public void testCDFFromSketchStrings()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        String sketch1 = hist1.toBase64String();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        String sketch2 = hist2.toBase64String();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        String sketch3 = hist3.toBase64String();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));
        double expectedCdf = combinedHist.cdf(2.0);
        assertAggregation(
                NETFLIX_QUERY_CDF_FROM_SKETCH_STRINGS_WITH_DEFAULTS,
                expectedCdf,
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3)),
                createRLEBlock(2.0, 3)
        );
    }

    @Test
    public void testCDFArrayFromSketchStrings()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        String sketch1 = hist1.toBase64String();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        String sketch2 = hist2.toBase64String();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        String sketch3 = hist3.toBase64String();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));
        assertAggregation(
                NETFLIX_QUERY_CDF_ARRAY_FROM_SKETCH_STRINGS_WITH_DEFAULTS,
                ImmutableList.of(
                        combinedHist.cdf(20.0),
                        combinedHist.cdf(30.0),
                        combinedHist.cdf(40.0),
                        combinedHist.cdf(50.0)
                ),
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3)),
                createRLEBlock(ImmutableList.of(20.0, 30.0, 40.0, 50.0), 3)
        );
    }

    @Test
    public void testApproxPercentileFromSketchStrings()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        String sketch1 = hist1.toBase64String();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        String sketch2 = hist2.toBase64String();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        String sketch3 = hist3.toBase64String();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));
        double expectedPercentile = combinedHist.quantile(0.18);
        assertAggregation(
                NETFLIX_QUERY_PERCENTILE_FROM_SKETCH_STRINGS_WITH_DEFAULTS,
                expectedPercentile,
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3)),
                createRLEBlock(0.18, 3)
        );

        List<Double> expectedPercentiles = ImmutableList.of(
                combinedHist.quantile(0.18),
                combinedHist.quantile(0.28),
                combinedHist.quantile(0.78),
                combinedHist.quantile(0.98)
        );
        assertAggregation(
                NETFLIX_QUERY_PERCENTILE_ARRAY_FROM_SKETCH_STRINGS_WITH_DEFAULTS,
                expectedPercentiles,
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3)),
                createRLEBlock(ImmutableList.of(0.18, 0.28, 0.78, 0.98), 3)
        );
    }

    @Test
    public void testPercentileFromSketchBytes()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        ByteBuffer sketch1 = hist1.toBytes();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        ByteBuffer sketch2 = hist2.toBytes();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        ByteBuffer sketch3 = hist3.toBytes();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));

        double expectedPercentile = combinedHist.quantile(0.5);
        assertAggregation(
                NETFLIX_QUERY_PERCENTILE_SKETCH_BYTES_WITH_DEFAULTS,
                expectedPercentile,
                createSlicesBlock(ImmutableList.of(
                        Slices.wrappedBuffer(sketch1),
                        Slices.wrappedBuffer(sketch2),
                        Slices.wrappedBuffer(sketch3)
                )),
                createRLEBlock(0.5, 3)
        );

        List<Double> expectedPercentiles = ImmutableList.of(
                combinedHist.quantile(0.18),
                combinedHist.quantile(0.28),
                combinedHist.quantile(0.78),
                combinedHist.quantile(0.98)
        );
        assertAggregation(
                NETFLIX_QUERY_PERCENTILE_ARRAY_SKETCH_BYTES_WITH_DEFAULTS,
                expectedPercentiles,
                createSlicesBlock(ImmutableList.of(
                        Slices.wrappedBuffer(sketch1),
                        Slices.wrappedBuffer(sketch2),
                        Slices.wrappedBuffer(sketch3)
                )),
                createRLEBlock(ImmutableList.of(0.18, 0.28, 0.78, 0.98), 3)
        );
    }

    @Test
    public void testQueryJSONHistogramSketchStrings()
            throws NetflixHistogramException
    {
        NetflixHistogram hist1 = new NetflixHistogram();
        hist1.add(1.0);
        hist1.add(1.0);
        hist1.add(2.0);
        String sketch1 = hist1.toBase64String();

        NetflixHistogram hist2 = new NetflixHistogram();
        hist2.add(2.0);
        hist2.add(2.0);
        hist2.add(2.0);
        String sketch2 = hist2.toBase64String();

        NetflixHistogram hist3 = new NetflixHistogram();
        hist3.add(3.0);
        hist3.add(4.0);
        hist3.add(5.0);
        hist3.add(5.0);
        hist3.add(5.0);
        String sketch3 = hist3.toBase64String();

        NetflixHistogram combinedHist = new NetflixHistogram(sketch1);
        combinedHist.add(new NetflixHistogram(sketch2));
        combinedHist.add(new NetflixHistogram(sketch3));

        String expectedJSON = combinedHist.getJsonHistogram();
        assertAggregation(
                NETFLIX_QUERY_JSON_HISTOGRAM_SKETCH_STRINGS_WITH_DEFAULTS,
                expectedJSON,
                createStringsBlock(ImmutableList.of(sketch1, sketch2, sketch3))
        );
    }
}
