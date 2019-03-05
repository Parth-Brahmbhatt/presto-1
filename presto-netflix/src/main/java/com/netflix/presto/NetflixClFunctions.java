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
package com.netflix.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.VarcharType;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;

public final class NetflixClFunctions
{
    private NetflixClFunctions()
    {
    }

    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    static {
        Configuration.setDefaults(new Configuration.Defaults()
        {
            private final JsonProvider jsonProvider = new JacksonJsonProvider(SORTED_MAPPER);
            private final MappingProvider mappingProvider = new JacksonMappingProvider();

            @Override
            public JsonProvider jsonProvider()
            {
                return jsonProvider;
            }

            @Override
            public MappingProvider mappingProvider()
            {
                return mappingProvider;
            }

            @Override
            public Set<Option> options()
            {
                return EnumSet.noneOf(Option.class);
            }
        });
    }

    private static Set<String> relationalOperators =
            new HashSet<>(Arrays.asList("==", "!=", ">=", "<=", ">", "<"));

    @VisibleForTesting
    static String generateClSnapshotJaywayJsonPath(Slice clTypeSlice, Slice extractCriteriaSlice, Slice filterCriteriaSlice)
    {
        String clType = (clTypeSlice == null) ? "" : clTypeSlice.toStringUtf8();
        String extractCriteria = (extractCriteriaSlice == null) ? "" : extractCriteriaSlice.toStringUtf8();
        String filterCriteria = (filterCriteriaSlice == null) ? "" : filterCriteriaSlice.toStringUtf8();

        String jsonPathifiedExtractCriteria = "";
        if (!extractCriteria.isEmpty()) {
            jsonPathifiedExtractCriteria = "." + extractCriteria;
        }

        String jsonPathifiedFilterCriteria = "";

        if (!filterCriteria.isEmpty()) {
            String[] tokens = filterCriteria
                    .split("((?<===|>|<|>=|<=|!=|&&|\\|\\||\\(|\\)) *|(?===|>|<|>=|<=|!=|&&|\\|\\||\\(|\\))) *");

            for (int i = 1; i < tokens.length; i++) {
                if (relationalOperators.contains(tokens[i])) {
                    tokens[i - 1] = "@." + tokens[i - 1].trim();
                }
            }

            StringBuilder filterCriteriaBuilder = new StringBuilder();
            filterCriteriaBuilder.append(" && (");
            for (String token : tokens) {
                filterCriteriaBuilder.append(token.trim());
            }
            filterCriteriaBuilder.append(")");

            jsonPathifiedFilterCriteria = filterCriteriaBuilder.toString();
        }

        return "$[?(\"" + clType + "\" in @.type"
                + jsonPathifiedFilterCriteria + ")]" + jsonPathifiedExtractCriteria;
    }

    @ScalarFunction("cl_snapshot_extract")
    @Description("Extracts values from CL snapshot based on supplied arguments, more information http://go/cludf")
    @SqlType("array(varchar)")
    public static Block snapshotExtract(@SqlType(VARCHAR) Slice snapshot, @SqlType(VARCHAR) Slice clType, @SqlType(VARCHAR) Slice extractCriteria, @SqlType(VARCHAR) Slice filterCriteria)
            throws IOException
    {
        if (clType == null || clType.toStringUtf8().isEmpty()) {
            throw new IllegalArgumentException("`cl_snapshot_extract` must be supplied with a valid `clType` from http://go/cl, refer http://go/cludf ");
        }

        try {
            String clSnapshotJsonPath = generateClSnapshotJaywayJsonPath(clType, extractCriteria, filterCriteria);
            List<Object> extractedValues = com.jayway.jsonpath.JsonPath.read(snapshot.getInput(), clSnapshotJsonPath);
            BlockBuilder parts = VarcharType.VARCHAR.createBlockBuilder(null, extractedValues.size());
            for (Object entry : extractedValues) {
                VarcharType.VARCHAR.writeSlice(parts, utf8Slice(JsonPath.parse(entry).jsonString()));
            }
            return parts.build();
        }
        catch (JsonPathException ex) {
            return null;
        }
    }
}
