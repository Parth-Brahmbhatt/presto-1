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

import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.netflix.presto.NetflixFunctions.addHoursToDateint;
import static com.netflix.presto.NetflixFunctions.dateDiff;
import static com.netflix.presto.NetflixFunctions.dateSub;
import static com.netflix.presto.NetflixFunctions.nfExtractJson;
import static com.netflix.presto.NetflixFunctions.nfExtractJsonScalar;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.metadata.FunctionExtractor.extractFunctions;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestNetflixFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        functionAssertions.addFunctions(extractFunctions(new NetflixPlugin().getFunctions()));
    }

    private static final String JSON = "{\"store\": {\"book\": [{\"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95}, " +
            "{\"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99}," +
            " {\"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99}, " +
            "{\"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}]," +
            "\"object\": {\"inner_object\": {\"array\": [{\"inner_array\": [{\"x\": \"y\"}]}]}}}}";

    @Test
    public void testDateSub()
    {
        assertEquals(dateSub(utf8Slice("20090730"), 3, utf8Slice("yyyyMMdd")), utf8Slice("20090727"));
        assertEquals(dateSub(utf8Slice("2009-07-30"), 1), utf8Slice("2009-07-29"));
        assertEquals(dateSub(20110817, 2, utf8Slice("yyyyMMdd")), utf8Slice("20110815"));
    }

    @Test
    public void testDateDiff()
    {
        assertEquals(dateDiff(utf8Slice("20090730"), utf8Slice("20090731"), utf8Slice("yyyyMMdd")), -1);
        assertEquals(dateDiff(20090730, 20090710, utf8Slice("yyyyMMdd")), 20);
        assertEquals(dateDiff(utf8Slice("2009-07-30"), utf8Slice("2009-07-25")), 5);
    }

    @Test
    public void testDateHourAdd()
    {
        assertEquals(addHoursToDateint(2011071818, 4), 2011071822);
    }

    @Test
    public void testJsonExtractMultiple()
    {
        assertFunction("JSON_EXTRACT_MULTIPLE('{\"x\":\"x_val\", \"y\":\"y_val\", \"z\":\"z_val\"}', '[\"x\", \"z\"]')", VARCHAR, "{\"x\":\"x_val\",\"z\":\"z_val\"}");
        assertFunction("JSON_EXTRACT_MULTIPLE('{\"x\":\"x_val\", \"y\":\"y_val\", \"z\":\"z_val\"}', '[\"x\", \"y\"]')", VARCHAR, "{\"x\":\"x_val\",\"y\":\"y_val\"}");
        assertFunction("JSON_EXTRACT_MULTIPLE('{\"x\":\"x_val\", \"y\":\"y_val\", \"z\":\"z_val\"}', '[\"y\"]')", VARCHAR, "{\"y\":\"y_val\"}");
        assertFunction("JSON_EXTRACT_MULTIPLE('{\"a\":1, \"b\": {\"a\" : 2}}', '[\"a\"]')", VARCHAR, "{\"a\":1}");
        assertEquals(NetflixFunctions.jsonExtractMultiple(wrappedBuffer("{\"x\":\"x_val\", \"y\":\"y_val\", \"z\":\"z_val\"}".getBytes()), wrappedBuffer("[\"DOESNTEXIST\"]".getBytes())), null);
        assertEquals(NetflixFunctions.jsonExtractMultiple(wrappedBuffer("\"\"".getBytes()), wrappedBuffer("[\"a\"]".getBytes())), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testInvalidJsonExtractMultipleKeys()
    {
        assertEquals(NetflixFunctions.jsonExtractMultiple(wrappedBuffer("".getBytes()), null), null);
    }

    @Test
    public void testJsonArrayStringHelper()
    {
        String[] res = NetflixFunctions.ArrayStringHelper.toStringArray("[\"a\", \"b\", \"c\"]");
        assertEquals(res.length, 3);
        assertEquals(res[0], "a");
        assertEquals(res[1], "b");
        assertEquals(res[2], "c");

        res = NetflixFunctions.ArrayStringHelper.toStringArray("[\"a\", \"b.c\", \"d\"]");
        assertEquals(res.length, 3);
        assertEquals(res[0], "a");
        assertEquals(res[1], "b.c");
        assertEquals(res[2], "d");

        res = NetflixFunctions.ArrayStringHelper.toStringArray("[\"a\", \"b,c\", \"d\"]");
        assertEquals(res.length, 3);
        assertEquals(res[0], "a");
        assertEquals(res[1], "b,c");
        assertEquals(res[2], "d");

        res = NetflixFunctions.ArrayStringHelper.toStringArray("[\"a\", \"b,c,d,e\", \"f;g;h\"]");
        assertEquals(res.length, 3);
        assertEquals(res[0], "a");
        assertEquals(res[1], "b,c,d,e");
        assertEquals(res[2], "f;g;h");
    }

    @Test
    public void testJsonExtract()
            throws IOException
    {
        assertJsonExtract(JSON, "$..book.length()", "[4]");
        assertJsonExtract(JSON, "$.store.object.inner_object.array[0].inner_array[0].x", "\"y\"");
        assertJsonExtractScalar(JSON, "$.store.object.inner_object.array[0].inner_array[0].x", "y");
        assertJsonExtract(JSON, "$.store.book[*].category", "[\"reference\",\"fiction\",\"fiction\",\"fiction\"]");
        assertJsonExtractScalar(JSON, "$.store.book[*].category", null);
        assertJsonExtract(JSON, "$.store.bicycle.price", null);
        assertJsonExtract("{\"s\":}", "$", null);
        assertJsonExtractScalar(JSON, "$.store.book[0].price", "8.95");
    }

    private void assertJsonExtract(String json, String jsonPath, String expected)
            throws IOException
    {
        Slice result = nfExtractJson(utf8Slice(json), utf8Slice(jsonPath));
        String actual = result == null ? null : result.toStringUtf8();
        assertEquals(actual, expected);
    }

    private void assertJsonExtractScalar(String json, String jsonPath, String expected)
            throws IOException
    {
        Slice result = nfExtractJsonScalar(utf8Slice(json), utf8Slice(jsonPath));
        String actual = result == null ? null : result.toStringUtf8();
        assertEquals(actual, expected);
    }
}
