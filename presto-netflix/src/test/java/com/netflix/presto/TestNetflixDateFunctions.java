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

import io.prestosql.Session;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static com.netflix.presto.NetflixDateFunctions.toDateInt;
import static com.netflix.presto.NetflixDateFunctions.toLocalDate;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.Hours.hoursBetween;
import static org.joda.time.Months.monthsBetween;
import static org.joda.time.Seconds.secondsBetween;
import static org.joda.time.Weeks.weeksBetween;

public class TestNetflixDateFunctions
        extends AbstractTestFunctions
{
    protected static final DateTimeZone UTC_TIME_ZONE = getDateTimeZone(UTC_KEY);
    private static final Integer DATEINT = 20180531;
    private static final String DATESTR = "2018-05-31";
    private static final DateTime DATE_1 = new DateTime(2018, 5, 31, 0, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime DATE_2 = new DateTime(2018, 6, 4, 0, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime DATE_3 = new DateTime(2018, 2, 1, 0, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime DATE_4 = new DateTime(2018, 5, 1, 0, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_1 = new DateTime(2018, 5, 31, 0, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_2 = new DateTime(2018, 5, 31, 12, 20, 21, 10, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_3 = new DateTime(2018, 5, 31, 22, 49, 33, 00, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_4 = new DateTime(2018, 3, 11, 1, 00, 00, 00, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_5 = new DateTime(2018, 3, 11, 5, 00, 00, 00, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_6 = new DateTime(2018, 5, 31, 7, 0, 0, 0, UTC_TIME_ZONE);
    private static final DateTime TIMESTAMP_7 = new DateTime(2018, 5, 31, 19, 20, 21, 10, UTC_TIME_ZONE);
    private final Session session;

    @BeforeClass
    public void setUp()
    {
        functionAssertions.installPlugin(new NetflixPlugin());
    }

    @Test
    public TestNetflixDateFunctions()
    {
        this(testSessionBuilder().setTimeZoneKey(UTC_KEY).build());
    }

    private TestNetflixDateFunctions(Session session)
    {
        super(session);
        this.session = session;
    }

    @Test
    public void testNfDateInt()
    {
        assertFunction("nf_dateint(20180531)", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('20180531')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('2018-05-31')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint(1527806973000)", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint(1527806973)", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint(date '2018-05-31')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint(timestamp '2018-05-31 12:20:21.010')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('2018-05-31T12:20:21.010')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('2018-05-31 12:20:21.010')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint(null)", IntegerType.INTEGER, null);
        assertFunction("nf_dateint('20183105', 'yyyyddMM')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint('20183105', null)", IntegerType.INTEGER, null);
        assertFunction("nf_dateint(20181531)", IntegerType.INTEGER, null);
        assertFunction("nf_dateint(timestamp '2018-05-31 02:41:06.000 Asia/Tokyo')", IntegerType.INTEGER, DATEINT);
        assertFunction("nf_dateint_today()", IntegerType.INTEGER, (int) toDateInt(epochLocalDateInZone(UTC_KEY, session.getStart().toEpochMilli())));
        assertFunction("nf_dateint('2019-12-05T18:04:27.603-08:00')", IntegerType.INTEGER, 20191206);
    }

    @Test
    public void testNfDateStr()
    {
        assertFunction("nf_datestr(20180531)", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('20180531')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('2018-05-31')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr(1527806973000)", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr(1527806973)", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr(date '2018-05-31')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr(timestamp '2018-05-31 12:20:21.010')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('2018-05-31T12:20:21.010')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('2018-05-31 12:20:21.010')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr(null)", VarcharType.VARCHAR, null);
        assertFunction("nf_datestr('20183105', 'yyyyddMM')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr('20183105', null)", VarcharType.VARCHAR, null);
        assertFunction("nf_datestr('20181531', null)", VarcharType.VARCHAR, null);
        assertFunction("nf_datestr(timestamp '2018-05-31 02:41:06.000 Asia/Tokyo')", VarcharType.VARCHAR, DATESTR);
        assertFunction("nf_datestr_today()", VarcharType.VARCHAR, epochLocalDateInZone(UTC_KEY, session.getStart().toEpochMilli()).toString());
    }

    @Test
    public void testNfToUnixTime()
    {
        assertFunction("nf_to_unixtime(20180531)", BigintType.BIGINT, TIMESTAMP_1.getMillis() / 1000);
        assertFunction("nf_to_unixtime(1527806973)", BigintType.BIGINT, 1527806973L);
        assertFunction("nf_to_unixtime(1527806973000)", BigintType.BIGINT, 1527806973L);
        assertFunction("nf_to_unixtime('20180531')", BigintType.BIGINT, TIMESTAMP_1.getMillis() / 1000);
        assertFunction("nf_to_unixtime('2018-05-31')", BigintType.BIGINT, TIMESTAMP_1.getMillis() / 1000);
        assertFunction("nf_to_unixtime(date '2018-05-31')", BigintType.BIGINT, TIMESTAMP_1.getMillis() / 1000);
        assertFunction("nf_to_unixtime(timestamp '2018-05-31 12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis() / 1000);
        assertFunction("nf_to_unixtime('2018-05-31T12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis() / 1000);
        assertFunction("nf_to_unixtime('2018-05-31 12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis() / 1000);
        assertFunction("nf_to_unixtime(null)", BigintType.BIGINT, null);
        assertFunction("nf_to_unixtime('20183105', 'yyyyddMM')", BigintType.BIGINT, TIMESTAMP_1.getMillis() / 1000);
        assertFunction("nf_to_unixtime('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", BigintType.BIGINT, TIMESTAMP_2.getMillis() / 1000);
        assertFunction("nf_to_unixtime('20183105', null)", BigintType.BIGINT, null);
        assertFunction("nf_to_unixtime_ms(20180531)", BigintType.BIGINT, TIMESTAMP_1.getMillis());
        assertFunction("nf_to_unixtime_ms('20180531')", BigintType.BIGINT, TIMESTAMP_1.getMillis());
        assertFunction("nf_to_unixtime_ms('2018-05-31')", BigintType.BIGINT, TIMESTAMP_1.getMillis());
        assertFunction("nf_to_unixtime_ms(date '2018-05-31')", BigintType.BIGINT, TIMESTAMP_1.getMillis());
        assertFunction("nf_to_unixtime_ms(timestamp '2018-05-31 12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis());
        assertFunction("nf_to_unixtime_ms('2018-05-31T12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis());
        assertFunction("nf_to_unixtime_ms('2018-05-31 12:20:21.010')", BigintType.BIGINT, TIMESTAMP_2.getMillis());
        assertFunction("nf_to_unixtime_ms(null)", BigintType.BIGINT, null);
        assertFunction("nf_to_unixtime_ms(20181531)", BigintType.BIGINT, null);
        assertFunction("nf_to_unixtime_ms('20183105', 'yyyyddMM')", BigintType.BIGINT, TIMESTAMP_1.getMillis());
        assertFunction("nf_to_unixtime_ms('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", BigintType.BIGINT, TIMESTAMP_2.getMillis());
        assertFunction("nf_to_unixtime_ms('20183105', null)", BigintType.BIGINT, null);
        assertFunction("nf_unixtime_now()", BigintType.BIGINT, session.getStart().getEpochSecond());
        assertFunction("nf_unixtime_now_ms()", BigintType.BIGINT, session.getStart().toEpochMilli());
    }

    @Test
    public void testNfFromUnixTime()
    {
        Date date = new Date(1527745543000L);
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
        sdformat.setTimeZone(TimeZone.getTimeZone(UTC_KEY.getId()));
        String res = sdformat.format(date);
        assertFunction("nf_from_unixtime(1527745543, 'yyyy/MM/dd')", VarcharType.VARCHAR, res);
        assertFunction("nf_from_unixtime(1527750000)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_6));
        assertFunction("nf_from_unixtime(1527794421010)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_7));
        assertFunction("nf_from_unixtime(549442800, 978307200)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_6));
        assertFunction("nf_from_unixtime_ms(549487221010, 978307200000)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_7));
        assertFunction("nf_from_unixtime(549442800, 978307200, 'yyyy/MM/dd')", VarcharType.VARCHAR, "2018/05/31");
        assertFunction("nf_from_unixtime_ms(549487221010, 978307200000, 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, toTimestamp(TIMESTAMP_7).toString());
        DateTime expected = new DateTime(2018, 5, 31, 10, 45, 43, DateTimeZone.forID("+05:00"));
        assertFunction("nf_from_unixtime_tz(1527745543, '+05:00')", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3), toTimestampWithTimeZone(expected));
        expected = new DateTime(2018, 5, 31, 7, 45, 43, DateTimeZone.forID("Europe/Paris"));
        assertFunction("nf_from_unixtime_ms_tz(1527745543000, 'Europe/Paris')", TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3), toTimestampWithTimeZone(expected));
        assertFunction("nf_from_unixtime_ms_tz_format(1527745543000, 'Europe/Paris', 'yyyy-MM-dd HH:mm:ss.SSSZZ')", VarcharType.VARCHAR, "2018-05-31 07:45:43.000+02:00");
        assertFunction("nf_from_unixtime_tz_format(1527745543, 'Europe/Paris', 'yyyy-MM-dd HH:mm:ss.SSSZZ')", VarcharType.VARCHAR, "2018-05-31 07:45:43.000+02:00");
    }

    @Test
    public void testNfDate()
    {
        assertFunction("nf_date(20180531)", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('20180531')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('2018-05-31')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date(1527806973000)", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date(1527806973)", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date(date '2018-05-31')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date(timestamp '2018-05-31 12:20:21.010')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('2018-05-31T12:20:21.010')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('2018-05-31 12:20:21.010')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date(null)", DateType.DATE, null);
        assertFunction("nf_date('20183105', 'yyyyddMM')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('31 May 2018', 'dd MMMMM yyyy')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('May 2018', 'MMMMM yyyy')", DateType.DATE, toDate(DATE_4));
        assertFunction("nf_date('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date('20183105', null)", DateType.DATE, null);
        assertFunction("nf_date(20181505)", DateType.DATE, null);
        assertFunction("nf_date(timestamp '2018-05-31 02:41:06.000 Asia/Tokyo')", DateType.DATE, toDate(DATE_1));
        assertFunction("nf_date_today()", DateType.DATE, toDate(new DateTime(session.getStart().toEpochMilli())));
    }

    @Test
    public void testNfTimestamp()
    {
        assertFunction("nf_timestamp(20180531)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_1));
        assertFunction("nf_timestamp('20180531')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_1));
        assertFunction("nf_timestamp('2018-05-31')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_1));
        assertFunction("nf_timestamp(1527806973000)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_3));
        assertFunction("nf_timestamp(1527806973)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_3));
        assertFunction("nf_timestamp(date '2018-05-31')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_1));
        assertFunction("nf_timestamp(timestamp '2018-05-31 12:20:21.010')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_2));
        assertFunction("nf_timestamp('2018-05-31T12:20:21.010')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_2));
        assertFunction("nf_timestamp('2018-05-31 12:20:21.010')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_2));
        assertFunction("nf_timestamp(null)", TIMESTAMP_MILLIS, null);
        assertFunction("nf_timestamp(20181531)", TIMESTAMP_MILLIS, null);
        assertFunction("nf_timestamp('2018-05-31X12:20:21.010')", TIMESTAMP_MILLIS, null);
        assertFunction("nf_timestamp('20183105', 'yyyyddMM')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_1));
        assertFunction("nf_timestamp('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_2));
        assertFunction("nf_timestamp('20183105', null)", TIMESTAMP_MILLIS, null);
        assertFunction("nf_timestamp_now()", TIMESTAMP_MILLIS, toTimestamp(new DateTime(session.getStart().toEpochMilli())));
    }

    @Test
    public void testNfDateAdd()
    {
        assertFunction("nf_dateadd(20180531, 2)", IntegerType.INTEGER, dateTimeToDateInt(DATE_1.plusDays(2)));
        assertFunction("nf_dateadd(1527794421, 2)", IntegerType.INTEGER, 1527967221);
        assertFunction("nf_dateadd(1527794421000, 2)", BigintType.BIGINT, 1527967221000L);
        assertFunction("nf_dateadd(cast(20180531 as bigint), 2)", BigintType.BIGINT, (long) dateTimeToDateInt(DATE_1.plusDays(2)));
        assertFunction("nf_dateadd('20180531', -2)", VarcharType.VARCHAR, String.valueOf(dateTimeToDateInt(DATE_1.plusDays(-2))));
        assertFunction("nf_dateadd('20180531', '2M')", VarcharType.VARCHAR, String.valueOf(dateTimeToDateInt(DATE_1.plusMonths(2))));
        assertFunction("nf_dateadd('day', 5, '20180531')", VarcharType.VARCHAR, String.valueOf(dateTimeToDateInt(DATE_1.plusDays(5))));
        assertFunction("nf_dateadd('2018-05-31', -2)", VarcharType.VARCHAR, toDate(DATE_1.plusDays(-2)).toString());
        assertFunction("nf_dateadd(date '2018-05-31', -2)", DateType.DATE, toDate(DATE_1.plusDays(-2)));
        assertFunction("nf_dateadd(timestamp '2018-05-31 12:20:21.010', -2)", TIMESTAMP_MILLIS, toTimestamp(TIMESTAMP_2.plusDays(-2)));
        assertFunction("nf_dateadd(timestamp '2018-05-31 12:20:21.010', null)", TIMESTAMP_MILLIS, null);
        assertFunction("nf_dateadd('hour', 2, timestamp '2018-03-11 01:00:00')", TimestampType.createTimestampType(0), SqlTimestamp.fromMillis(0, TIMESTAMP_4.plusHours(2).getMillis()));
        //assertFunction("nf_dateadd('2018-05-31 12:20:21.010', -2)", VarcharType.VARCHAR, TIMESTAMP_2.plusDays(-2).toString());
        //assertFunction("nf_dateadd('2018-05-31 12:20:21.010', '-2d')", VarcharType.VARCHAR, TIMESTAMP_2.plusDays(-2).toString());
        assertFunction("nf_dateadd('week', -10, '2018-05-31 12:20:21.010')", VarcharType.VARCHAR, TIMESTAMP_2.plusWeeks(-10).toString());
    }

    @Test
    public void testNfDateDiff()
    {
        assertFunction("nf_datediff(20180531, 20180604)", BigintType.BIGINT, (long) daysBetween(DATE_1, DATE_2).getDays());
        assertFunction("nf_datediff('2018-06-04', '2018-05-31')", BigintType.BIGINT, (long) daysBetween(DATE_2, DATE_1).getDays());
        assertFunction("nf_datediff('month', '20180201', '20180531')", BigintType.BIGINT, (long) monthsBetween(DATE_3, DATE_1).getMonths());
        assertFunction("nf_datediff(date '2018-06-04', date '2018-05-31')", BigintType.BIGINT, (long) daysBetween(DATE_2, DATE_1).getDays());
        assertFunction("nf_datediff('week', '20180531', '20180201')", BigintType.BIGINT, (long) weeksBetween(DATE_1, DATE_3).getWeeks());
        assertFunction("nf_datediff('second', timestamp '2018-05-31 12:20:21.010', timestamp '2018-05-31 22:49:33')", BigintType.BIGINT, (long) secondsBetween(TIMESTAMP_2, TIMESTAMP_3).getSeconds());
        assertFunction("nf_datediff(timestamp '2018-05-31 12:20:21.010', null)", BigintType.BIGINT, null);
        assertFunction("nf_datediff(1531373400000, 1531546200000)", BigintType.BIGINT, 2L);
        assertFunction("nf_datediff(1531373400, 1531546200)", BigintType.BIGINT, 2L);
        assertFunction("nf_datediff(1531373400000, 1531546200)", BigintType.BIGINT, 2L);
        assertFunction("nf_datediff('hour', '2018-03-11 05:00:00', '2018-03-11 01:00:00')", BigintType.BIGINT, (long) hoursBetween(TIMESTAMP_5, TIMESTAMP_4).getHours());
    }

    @Test
    public void testNfDateTrunc()
    {
        DateTime result = DATE_1;
        result = result.withDayOfMonth(1);
        assertFunction("nf_datetrunc('month', 20180531)", IntegerType.INTEGER, dateTimeToDateInt(result));
        assertFunction("nf_datetrunc('month','20180531')", VarcharType.VARCHAR, String.valueOf(dateTimeToDateInt(result)));
        result = result.withMonthOfYear(1);
        assertFunction(" nf_datetrunc('year','2018-05-31')", VarcharType.VARCHAR, toDate(result).toString());
        DateTime resultTs = TIMESTAMP_7;
        assertFunction("nf_datetrunc('millisecond', '2018-05-31T19:20:21.010')", VarcharType.VARCHAR, resultTs.toString());
        resultTs = resultTs.withSecondOfMinute(0);
        resultTs = resultTs.withMillisOfSecond(0);
        assertFunction("nf_datetrunc('minute','2018-05-31 19:20:21.010')", VarcharType.VARCHAR, resultTs.toString());
        assertFunction("nf_datetrunc('minute', 1527794421)", IntegerType.INTEGER, (int) (resultTs.getMillis() / 1000));
        assertFunction("nf_datetrunc('minute', 1527794421000)", BigintType.BIGINT, resultTs.getMillis());
        resultTs = resultTs.withHourOfDay(0);
        resultTs = resultTs.withMinuteOfHour(0);
        assertFunction("nf_datetrunc('day', timestamp '2018-05-31 19:20:21.010')", TIMESTAMP_MILLIS, toTimestamp(resultTs));
        assertFunction("nf_datetrunc(null, 20180531)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfDateFormat()
    {
        String result = "2018-05-31 00:00:00.000";
        assertFunction("nf_dateformat(20180531, 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat('20180531', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat('2018-05-31', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat(date '2018-05-31', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        result = "2018-05-31 19:20:21.010";
        assertFunction("nf_dateformat(timestamp '2018-05-31 19:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat(1527794421010, 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat('2018-05-31T19:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat('2018-05-31 19:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        result = "2018-05-31 19:20:21.000";
        assertFunction("nf_dateformat(1527794421, 'yyyy-MM-dd HH:mm:ss.SSS')", VarcharType.VARCHAR, result);
        assertFunction("nf_dateformat('20180531', null)", VarcharType.VARCHAR, null);
    }

    @Test
    public void testNfYear()
    {
        int year = 2018;
        assertFunction("nf_year(20180531)", IntegerType.INTEGER, year);
        assertFunction("nf_year('20180531')", IntegerType.INTEGER, year);
        assertFunction("nf_year('2018-05-31')", IntegerType.INTEGER, year);
        assertFunction("nf_year(1527806973000)", IntegerType.INTEGER, year);
        assertFunction("nf_year(1527806973)", IntegerType.INTEGER, year);
        assertFunction("nf_year(date '2018-05-31')", IntegerType.INTEGER, year);
        assertFunction("nf_year(timestamp '2018-05-31 12:20:21.010')", IntegerType.INTEGER, year);
        assertFunction("nf_year('2018-05-31T12:20:21.010')", IntegerType.INTEGER, year);
        assertFunction("nf_year('2018-05-31 12:20:21.010')", IntegerType.INTEGER, year);
        assertFunction("nf_year(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfMonth()
    {
        int month = 5;
        assertFunction("nf_month(20180531)", IntegerType.INTEGER, month);
        assertFunction("nf_month('20180531')", IntegerType.INTEGER, month);
        assertFunction("nf_month('2018-05-31')", IntegerType.INTEGER, month);
        assertFunction("nf_month(1527806973000)", IntegerType.INTEGER, month);
        assertFunction("nf_month(1527806973)", IntegerType.INTEGER, month);
        assertFunction("nf_month(date '2018-05-31')", IntegerType.INTEGER, month);
        assertFunction("nf_month('2018-05-31T12:20:21.010')", IntegerType.INTEGER, month);
        assertFunction("nf_month('2018-05-31 12:20:21.010')", IntegerType.INTEGER, month);
        month = 1;
        assertFunction("nf_month(timestamp '2018-01-01 00:00:00.000')", IntegerType.INTEGER, month);
        assertFunction("nf_month(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfDay()
    {
        int day = 31;
        assertFunction("nf_day(20180531)", IntegerType.INTEGER, day);
        assertFunction("nf_day('20180531')", IntegerType.INTEGER, day);
        assertFunction("nf_day('2018-05-31')", IntegerType.INTEGER, day);
        assertFunction("nf_day(1527806973000)", IntegerType.INTEGER, day);
        assertFunction("nf_day(1527806973)", IntegerType.INTEGER, day);
        assertFunction("nf_day(date '2018-05-31')", IntegerType.INTEGER, day);
        assertFunction("nf_day('2018-05-31T12:20:21.010')", IntegerType.INTEGER, day);
        assertFunction("nf_day('2018-05-31 12:20:21.010')", IntegerType.INTEGER, day);
        day = 1;
        assertFunction("nf_day(timestamp '2018-01-01 00:00:00.000')", IntegerType.INTEGER, day);
        assertFunction("nf_day(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfDayOfWeek()
    {
        assertFunction("nf_day_of_week(20180603)", IntegerType.INTEGER, 7);
        assertFunction("nf_day_of_week('20180603')", IntegerType.INTEGER, 7);
        assertFunction("nf_day_of_week('2018-06-03')", IntegerType.INTEGER, 7);
        assertFunction("nf_day_of_week(1591045342000)", IntegerType.INTEGER, 1);
        assertFunction("nf_day_of_week(1591045342)", IntegerType.INTEGER, 1);
        assertFunction("nf_day_of_week(date '2018-06-03')", IntegerType.INTEGER, 7);
        assertFunction("nf_day_of_week('2020-06-01T12:20:21.010')", IntegerType.INTEGER, 1);
        assertFunction("nf_day_of_week('2020-06-01 12:20:21.010')", IntegerType.INTEGER, 1);
        assertFunction("nf_day_of_week(timestamp '2018-01-01 00:00:00.000')", IntegerType.INTEGER, 1);
        assertFunction("nf_day_of_week(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfWeek()
    {
        int week = 22;
        assertFunction("nf_week(20180531)", IntegerType.INTEGER, week);
        assertFunction("nf_week('20180531')", IntegerType.INTEGER, week);
        assertFunction("nf_week('2018-05-31')", IntegerType.INTEGER, week);
        assertFunction("nf_week(1527806973000)", IntegerType.INTEGER, week);
        assertFunction("nf_week(1527806973)", IntegerType.INTEGER, week);
        assertFunction("nf_week(date '2018-05-31')", IntegerType.INTEGER, week);
        assertFunction("nf_week('2018-05-31T12:20:21.010')", IntegerType.INTEGER, week);
        assertFunction("nf_week('2018-05-31 12:20:21.010')", IntegerType.INTEGER, week);
        week = 1;
        assertFunction("nf_week(timestamp '2018-01-01 00:00:00.000')", IntegerType.INTEGER, week);
        assertFunction("nf_week(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfQuarter()
    {
        int quarter = 2;
        assertFunction("nf_quarter(20180531)", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter('20180531')", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter('2018-05-31')", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter(1527806973000)", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter(1527806973)", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter(date '2018-05-31')", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter('2018-05-31T12:20:21.010')", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter('2018-05-31 12:20:21.010')", IntegerType.INTEGER, quarter);
        quarter = 1;
        assertFunction("nf_quarter(timestamp '2018-01-01 00:00:00.000')", IntegerType.INTEGER, quarter);
        assertFunction("nf_quarter(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfHour()
    {
        int hour = 0;
        assertFunction("nf_hour(20180531)", IntegerType.INTEGER, hour);
        assertFunction("nf_hour('20180531')", IntegerType.INTEGER, hour);
        assertFunction("nf_hour('2018-05-31')", IntegerType.INTEGER, hour);
        assertFunction("nf_hour(date '2018-05-31')", IntegerType.INTEGER, hour);
        hour = 12;
        assertFunction("nf_hour('2018-05-31T12:20:21.010')", IntegerType.INTEGER, hour);
        assertFunction("nf_hour('2018-05-31 12:20:21.010')", IntegerType.INTEGER, hour);
        hour = 22;
        assertFunction("nf_hour(1527806973000)", IntegerType.INTEGER, hour);
        assertFunction("nf_hour(1527806973)", IntegerType.INTEGER, hour);
        hour = 21;
        assertFunction("nf_hour(timestamp '2018-01-01 21:00:00.000')", IntegerType.INTEGER, hour);
        assertFunction("nf_hour(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfMinute()
    {
        int minute = 0;
        assertFunction("nf_minute(20180531)", IntegerType.INTEGER, minute);
        assertFunction("nf_minute('20180531')", IntegerType.INTEGER, minute);
        assertFunction("nf_minute('2018-05-31')", IntegerType.INTEGER, minute);
        assertFunction("nf_minute(date '2018-05-31')", IntegerType.INTEGER, minute);
        minute = 20;
        assertFunction("nf_minute('2018-05-31T12:20:21.010')", IntegerType.INTEGER, minute);
        assertFunction("nf_minute('2018-05-31 12:20:21.010')", IntegerType.INTEGER, minute);
        minute = 49;
        assertFunction("nf_minute(1527806973000)", IntegerType.INTEGER, minute);
        assertFunction("nf_minute(1527806973)", IntegerType.INTEGER, minute);
        minute = 5;
        assertFunction("nf_minute(timestamp '2018-01-01 21:05:00.000')", IntegerType.INTEGER, minute);
        assertFunction("nf_minute(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfSecond()
    {
        int second = 0;
        assertFunction("nf_second(20180531)", IntegerType.INTEGER, second);
        assertFunction("nf_second('20180531')", IntegerType.INTEGER, second);
        assertFunction("nf_second('2018-05-31')", IntegerType.INTEGER, second);
        assertFunction("nf_second(date '2018-05-31')", IntegerType.INTEGER, second);
        second = 21;
        assertFunction("nf_second('2018-05-31T12:20:21.010')", IntegerType.INTEGER, second);
        assertFunction("nf_second('2018-05-31 12:20:21.010')", IntegerType.INTEGER, second);
        second = 33;
        assertFunction("nf_second(1527806973000)", IntegerType.INTEGER, second);
        assertFunction("nf_second(1527806973)", IntegerType.INTEGER, second);
        second = 10;
        assertFunction("nf_second(timestamp '2018-01-01 21:05:10.000')", IntegerType.INTEGER, second);
        assertFunction("nf_second(null)", IntegerType.INTEGER, null);
    }

    @Test
    public void testNfMilliSecond()
    {
        int millisecond = 0;
        assertFunction("nf_millisecond(20180531)", IntegerType.INTEGER, millisecond);
        assertFunction("nf_millisecond('20180531')", IntegerType.INTEGER, millisecond);
        assertFunction("nf_millisecond('2018-05-31')", IntegerType.INTEGER, millisecond);
        assertFunction("nf_millisecond(date '2018-05-31')", IntegerType.INTEGER, millisecond);
        millisecond = 10;
        assertFunction("nf_millisecond('2018-05-31T12:20:21.010')", IntegerType.INTEGER, millisecond);
        assertFunction("nf_millisecond('2018-05-31 12:20:21.010')", IntegerType.INTEGER, millisecond);
        millisecond = 210;
        assertFunction("nf_millisecond(timestamp '2018-01-01 21:05:10.210')", IntegerType.INTEGER, millisecond);
        assertFunction("nf_millisecond(null)", IntegerType.INTEGER, null);
    }

    private static SqlTimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return SqlTimestampWithTimeZone.newInstance(3, dateTime.getMillis(), 0, getTimeZoneKey(dateTime.getZone().toTimeZone().getID()));
    }

    private static SqlDate toDate(DateTime dateDate)
    {
        long epochDays = epochLocalDateInZone(UTC_KEY, dateDate.getMillis()).toEpochDay();
        return new SqlDate(toIntExact(epochDays));
    }

    private static LocalDate epochLocalDateInZone(TimeZoneKey timeZoneKey, long instant)
    {
        return LocalDate.from(Instant.ofEpochMilli(instant).atZone(ZoneId.of(timeZoneKey.getId())));
    }

    private static SqlTimestamp toTimestamp(DateTime dateTime)
    {
        return SqlTimestamp.fromMillis(3, dateTime.getMillis());
    }

    private static int dateTimeToDateInt(DateTime dateTime)
    {
        return (int) toDateInt(toLocalDate(dateTime.getMillis(), UTC_KEY));
    }
}
