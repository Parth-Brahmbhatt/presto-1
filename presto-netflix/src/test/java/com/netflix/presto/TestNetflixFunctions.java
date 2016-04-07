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

import org.testng.annotations.Test;

import static com.netflix.presto.NetflixFunctions.addHoursToDateint;
import static com.netflix.presto.NetflixFunctions.dateDiff;
import static com.netflix.presto.NetflixFunctions.dateSub;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestNetflixFunctions
{
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
}
