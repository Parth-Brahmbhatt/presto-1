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
import io.prestosql.spi.block.Block;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static com.netflix.presto.NetflixClFunctions.generateClSnapshotJaywayJsonPath;
import static com.netflix.presto.NetflixClFunctions.snapshotExtract;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestNetflixClFunctions
        extends AbstractTestFunctions
{
    private String snapshot;

    @BeforeTest
    void setSnapshot()
    {
        this.snapshot = "[" +
                "    {\"sequence\":1,\"id\":757294327146741,\"source\":\"iOS\",\"schema\":{\"name\":\"App\",\"version\":\"1.26.0\"},\"type\":[\"Log\",\"Session\"],\"time\":559440041005}," +
                "    {\"type\":[\"ProcessState\"],\"id\":757294397275504,\"computation\":\"none\",\"allocation\":\"none\",\"interaction\":\"none\"}," +
                "    {\"type\":[\"ProcessState\"],\"id\":757294425125683,\"computation\":\"normal\",\"allocation\":\"normal\",\"interaction\":\"direct\"}," +
                "    {\"sequence\":3,\"id\":757294439218544,\"type\":[\"ProcessStateTransition\",\"Action\",\"Session\"],\"time\":559440041006}," +
                "    {\"nfvdid\":\"blahblablb\",\"id\":757294485523660,\"sequence\":4,\"type\":[\"VisitorDeviceId\",\"AccountIdentity\",\"Session\"],\"time\":559440041008}," +
                "    {\"sequence\":5,\"id\":757294513373839,\"type\":[\"UserInteraction\",\"Session\"],\"time\":559440041008}," +
                "    {\"model\":\"APPLE_iPhone9-1\",\"type\":[\"Device\"],\"id\":757294562698854}," +
                "    {\"type\":[\"OsVersion\"],\"osVersion\":\"11.4.1\",\"id\":757294575785082}," +
                "    {\"appVersion\":\"11.2.0\",\"type\":[\"AppVersion\"],\"id\":757294615379312}," +
                "    {\"uiVersion\":\"ios11.2.0 (2265)\",\"type\":[\"UiVersion\"],\"id\":757294643565035}," +
                "    {\"esn\":\"\",\"type\":[\"Esn\"],\"id\":757294732484280}," +
                "    {\"type\":[\"NrdLib\"],\"id\":757294743557242,\"appVersion\":\"11.2.0 (2265)\",\"sdkVersion\":\"2012.4\",\"libVersion\":\"2012.4\"}," +
                "    {\"userAgent\":\" App/11.2.0 ( iOS 11.4.1 )\",\"type\":[\"UserAgent\"],\"id\":757294786171371}," +
                "    {\"utcOffset\":-18000,\"type\":[\"TimeZone\"],\"id\":757294829121044}," +
                "    {\"muting\":false,\"id\":757294867037552,\"level\":0.875,\"type\":[\"Volume\"]}," +
                "    {\"type\":[\"WifiConnection\",\"NetworkConnection\"],\"id\":757294904954060}," +
                "    {\"type\":[\"UiLocale\"],\"uiLocale\":\"en\",\"id\":757294954950164}," +
                "    {\"cells\":{\"7972\":1,\"10953\":4},\"type\":[\"TestAllocations\"],\"id\":757294995551027}," +
                "    {\"trackingInfo\":{\"videoId\":12345,\"trackId\":9087,\"imageKey\":\"test1\"},\"sequence\":9,\"id\":757295011213817,\"view\":\"browseTitles\",\"type\":[\"Presentation\",\"Session\"],\"time\":559440043683}," +
                "    {\"trackingInfo\":{\"surveyResponse\":1,\"surveyIdentifier\":\"IO_80203147\"},\"sequence\":11,\"id\":757295111313817,\"view\":\"homeTab\",\"type\":[\"Focus\",\"Session\"],\"time\":559440043683}" +
                "]";
    }

    @Test
    public void testGetClSnapshotJsonPathWithOnlyEventType()
    {
        assertGenerateClSnapshotJaywayJsonPath("Focus", null, null, "$[?(\"Focus\" in @.type)]");
        assertGenerateClSnapshotJaywayJsonPath("Session", null, null, "$[?(\"Session\" in @.type)]");
    }

    @Test
    public void testGetClSnapshotJsonPathWithEventTypeAndExtractCriteria()
    {
        assertGenerateClSnapshotJaywayJsonPath("Focus", "view", null, "$[?(\"Focus\" in @.type)].view");
        assertGenerateClSnapshotJaywayJsonPath("Focus", "trackingInfo.surveyResponse", null, "$[?(\"Focus\" in @.type)].trackingInfo.surveyResponse");
    }

    @Test
    public void testGetClSnapshotJsonPathWithAllArguments()
    {
        assertGenerateClSnapshotJaywayJsonPath("Focus", "trackingInfo", "view==\"homeTab\"", "$[?(\"Focus\" in @.type && (@.view==\"homeTab\"))].trackingInfo");

        assertGenerateClSnapshotJaywayJsonPath("Focus", "trackingInfo.surveyIdentifier", "view==\"homeTab\" && trackingInfo.surveyResponse==1",
                "$[?(\"Focus\" in @.type && (@.view==\"homeTab\"&&@.trackingInfo.surveyResponse==1))].trackingInfo.surveyIdentifier");

        assertGenerateClSnapshotJaywayJsonPath("Presentation", "trackingInfo.imageKey", "view==\"browseTitles\" && ( trackingInfo.trackId==9087 || trackingInfo.videoId == 12345 )",
                "$[?(\"Presentation\" in @.type && (@.view==\"browseTitles\"&&(@.trackingInfo.trackId==9087||@.trackingInfo.videoId==12345)))].trackingInfo.imageKey");
    }

    @Test
    public void testSnapshotExtractWithClTypeAlone()
            throws IOException
    {
        ArrayList<String> result1 = new ArrayList<>(Arrays.asList("{\"id\":757295111313817,\"sequence\":11,\"time\":559440043683,\"trackingInfo\":{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1},\"type\":[\"Focus\",\"Session\"],\"view\":\"homeTab\"}"));
        assertSnapshotExtract(this.snapshot, "Focus", "", "", result1);
        ArrayList<String> result2 = new ArrayList<>(Arrays.asList("{\"id\":757294327146741,\"schema\":{\"name\":\"App\",\"version\":\"1.26.0\"},\"sequence\":1,\"source\":\"iOS\",\"time\":559440041005,\"type\":[\"Log\",\"Session\"]}",
                "{\"id\":757294439218544,\"sequence\":3,\"time\":559440041006,\"type\":[\"ProcessStateTransition\",\"Action\",\"Session\"]}",
                "{\"id\":757294485523660,\"nfvdid\":\"blahblablb\",\"sequence\":4,\"time\":559440041008,\"type\":[\"VisitorDeviceId\",\"AccountIdentity\",\"Session\"]}",
                "{\"id\":757294513373839,\"sequence\":5,\"time\":559440041008,\"type\":[\"UserInteraction\",\"Session\"]}",
                "{\"id\":757295011213817,\"sequence\":9,\"time\":559440043683,\"trackingInfo\":{\"imageKey\":\"test1\",\"trackId\":9087,\"videoId\":12345},\"type\":[\"Presentation\",\"Session\"],\"view\":\"browseTitles\"}",
                "{\"id\":757295111313817,\"sequence\":11,\"time\":559440043683,\"trackingInfo\":{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1},\"type\":[\"Focus\",\"Session\"],\"view\":\"homeTab\"}"));
        assertSnapshotExtract(this.snapshot, "Session", "", "", result2);
    }

    @Test
    public void testSnapshotExtractWithClTypeAndExtractCriteria()
            throws IOException
    {
        ArrayList<String> result1 = new ArrayList<>(Arrays.asList("\"homeTab\""));
        assertSnapshotExtract(this.snapshot, "Focus", "view", "", result1);
        ArrayList<String> result2 = new ArrayList<>(Arrays.asList("1"));
        assertSnapshotExtract(this.snapshot, "Focus", "trackingInfo.surveyResponse", "", result2);
    }

    @Test
    public void testSnapshotExtractWithAllArguments()
            throws IOException
    {
        ArrayList<String> result1 = new ArrayList<>(Arrays.asList("{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1}"));
        assertSnapshotExtract(this.snapshot, "Focus", "trackingInfo", "view==\"homeTab\"", result1);
        ArrayList<String> result2 = new ArrayList<>(Arrays.asList("\"IO_80203147\""));
        assertSnapshotExtract(this.snapshot, "Focus", "trackingInfo.surveyIdentifier", "view==\"homeTab\" && trackingInfo.surveyResponse==1", result2);
        ArrayList<String> result3 = new ArrayList<>();
        assertSnapshotExtract(this.snapshot, "Focus", "trackingInfo.surveyIdentifier", "view==\"browseTitles\" && trackingInfo.surveyResponse==1", result3);
        ArrayList<String> result4 = new ArrayList<>(Arrays.asList("\"test1\""));
        assertSnapshotExtract(this.snapshot, "Presentation", "trackingInfo.imageKey", "view==\"browseTitles\" && ( trackingInfo.trackId==9087 || trackingInfo.videoId == 12345 )", result4);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSnapshotExtractWithoutClTypeArgument()
            throws IOException
    {
        snapshotExtract(utf8Slice(snapshot), utf8Slice(""), utf8Slice("trackingInfo.imageKey"), utf8Slice(""));
        snapshotExtract(utf8Slice(snapshot), null, utf8Slice("trackingInfo.imageKey"), utf8Slice(""));
    }

    private void assertSnapshotExtract(String snapshot, String clType, String extractCriteria, String filterCriteria, ArrayList<String> expected)
            throws IOException
    {
        Block result = snapshotExtract(utf8Slice(snapshot), utf8Slice(clType), utf8Slice(extractCriteria), utf8Slice(filterCriteria));
        assertNotNull(result);
        ArrayList<String> actual = new ArrayList<>();
        for (int i = 0; i < result.getPositionCount(); i++) {
            actual.add(result.getSlice(i, 0, result.getSliceLength(i)).toStringUtf8());
        }
        assertEquals(actual, expected);
    }

    private void assertGenerateClSnapshotJaywayJsonPath(String clType, String extractCriteria, String filterCriteria, String expected)
    {
        Slice clTypeSlice = clType == null ? null : utf8Slice(clType);
        Slice extractCriteriaSlice = extractCriteria == null ? null : utf8Slice(extractCriteria);
        Slice filterCriteriaSlice = filterCriteria == null ? null : utf8Slice(filterCriteria);
        assertEquals(generateClSnapshotJaywayJsonPath(clTypeSlice, extractCriteriaSlice, filterCriteriaSlice), expected);
    }
}
