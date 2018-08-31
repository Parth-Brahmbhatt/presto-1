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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static java.util.Objects.requireNonNull;

/**
 * The implementations of these functions are copied from https://stash.corp.netflix.com/projects/BDP/repos/hive-udf/
 */
public final class NetflixFunctions
{
    private static final int SECONDS_PER_DAY = 86_400;

    private NetflixFunctions()
    {
    }

    private static Slice dateSub(String dateString, long days, Optional<Slice> format)
    {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat formatter;
        if (format.isPresent()) {
            formatter = new SimpleDateFormat(format.get().toStringUtf8());
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }

        try {
            calendar.setTime(formatter.parse(dateString));
            calendar.add(Calendar.DAY_OF_MONTH, -(int) days);
            Date newDate = calendar.getTime();
            return utf8Slice(formatter.format(newDate));
        }
        catch (ParseException e) {
            return null;
        }
    }

    @Description("subtracts the given number of days from the given date")
    @ScalarFunction("date_sub")
    @SqlType(VARCHAR)
    public static Slice dateSub(@SqlType(VARCHAR) Slice dateString, @SqlType(BIGINT) long days, @SqlType(VARCHAR) Slice format)
    {
        return dateSub(dateString.toStringUtf8(), days, Optional.of(format));
    }

    @Description("subtracts the given number of days from the given date")
    @ScalarFunction("date_sub")
    @SqlType(VARCHAR)
    public static Slice dateSub(@SqlType(BIGINT) long dateInt, @SqlType(BIGINT) long days, @SqlType(VARCHAR) Slice format)
    {
        return dateSub(String.valueOf(dateInt), days, Optional.of(format));
    }

    @Description("subtracts the given number of days from the given date")
    @ScalarFunction("date_sub")
    @SqlType(VARCHAR)
    public static Slice dateSub(@SqlType(VARCHAR) Slice dateString, @SqlType(BIGINT) long days)
    {
        return dateSub(dateString.toStringUtf8(), days, Optional.empty());
    }

    private static long dateDiff(String dateString1, String dateString2, Optional<Slice> format)
    {
        SimpleDateFormat formatter;
        if (format.isPresent()) {
            formatter = new SimpleDateFormat(format.get().toStringUtf8());
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }

        try {
            // NOTE: This implementation avoids the extra-second problem
            // by comparing with UTC epoch and integer division.
            long diffInMilliSeconds = (formatter.parse(dateString1)
                    .getTime() - formatter.parse(dateString2).getTime());

            return (diffInMilliSeconds / (SECONDS_PER_DAY * 1000));
        }
        catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("days between given days")
    @ScalarFunction("datediff")
    @SqlType(BIGINT)
    public static long dateDiff(@SqlType(VARCHAR) Slice dateString1, @SqlType(VARCHAR) Slice dateString2, @SqlType(VARCHAR) Slice format)
    {
        return dateDiff(dateString1.toStringUtf8(), dateString2.toStringUtf8(), Optional.of(format));
    }

    @Description("days between given days")
    @ScalarFunction("datediff")
    @SqlType(BIGINT)
    public static long dateDiff(@SqlType(BIGINT) long date1, @SqlType(BIGINT) long date2, @SqlType(VARCHAR) Slice format)
    {
        return dateDiff(String.valueOf(date1), String.valueOf(date2), Optional.of(format));
    }

    @Description("days between given days")
    @ScalarFunction("datediff")
    @SqlType(BIGINT)
    public static long dateDiff(@SqlType(VARCHAR) Slice dateString1, @SqlType(VARCHAR) Slice dateString2)
    {
        return dateDiff(dateString1.toStringUtf8(), dateString2.toStringUtf8(), Optional.empty());
    }

    @Description("current date as yyyyMMdd. e.g., 20160406. Example: select * from tableName where dateint=dateint_today().")
    @ScalarFunction("dateint_today")
    @SqlType(BIGINT)
    public static long dateintToday(ConnectorSession session)
    {
        // it's OK to use session start time (created using local jvm timezone) as
        // presto will be running on linux configured with utc timezone
        long sessionStartTimeMillis = session.getStartTime();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(sessionStartTimeMillis);
        return calendar.get(YEAR) * 10000 + (calendar.get(MONTH) + 1) * 100 + calendar.get(DAY_OF_MONTH);
    }

    @Description("add (subtract if negative) hours to given dateint hour")
    @ScalarFunction("datehour_add")
    @SqlType(BIGINT)
    public static long addHoursToDateint(@SqlType(BIGINT) long dateintHour, @SqlType(BIGINT) long deltaHours)
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
        Calendar calendar = Calendar.getInstance();

        try {
            calendar.setTime(formatter.parse(String.valueOf(dateintHour)));
            calendar.add(HOUR_OF_DAY, (int) deltaHours);
        }
        catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        return Long.parseLong(formatter.format(calendar.getTime()));
    }

    private static long dateintToUnixTimestamp(String dateintString, Optional<Slice> format)
    {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat formatter;

        if (format.isPresent()) {
            formatter = new SimpleDateFormat(format.get().toStringUtf8());
        }
        else {
            formatter = new SimpleDateFormat("yyyyMMdd");
        }

        try {
            calendar.setTime(formatter.parse(dateintString));
        }
        catch (ParseException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }

        return calendar.getTimeInMillis() / 1000;
    }

    @Description("convert dateint to Unix timestamp")
    @ScalarFunction("dateint_to_unixts")
    @SqlType(BIGINT)
    public static long dateintToUnixTimestamp(@SqlType(VARCHAR) Slice dateintString, @SqlType(VARCHAR) Slice format)
    {
        return dateintToUnixTimestamp(dateintString.toStringUtf8(), Optional.of(format));
    }

    @Description("convert dateint to Unix timestamp")
    @ScalarFunction("dateint_to_unixts")
    @SqlType(BIGINT)
    public static long dateintToUnixTimestamp(@SqlType(BIGINT) long dateint, @SqlType(VARCHAR) Slice format)
    {
        return dateintToUnixTimestamp(String.valueOf(dateint), Optional.of(format));
    }

    @Description("convert dateint to Unix timestamp")
    @ScalarFunction("dateint_to_unixts")
    @SqlType(BIGINT)
    public static long dateintToUnixTimestamp(@SqlType(VARCHAR) Slice dateintString)
    {
        return dateintToUnixTimestamp(dateintString.toStringUtf8(), Optional.empty());
    }

    @Description("convert dateint to Unix timestamp")
    @ScalarFunction("dateint_to_unixts")
    @SqlType(BIGINT)
    public static long dateintToUnixTimestamp(@SqlType(BIGINT) long dateint)
    {
        return dateintToUnixTimestamp(String.valueOf(dateint), Optional.empty());
    }

    @Description("current hour. e.g., 5. Example: select * from tableName where hour=hour_now().")
    @ScalarFunction("hour_now")
    @SqlType(BIGINT)
    public static long hourNow(ConnectorSession session)
    {
        // it's OK to use session start time (created using local jvm timezone) as
        // presto will be running on linux configured with utc timezone
        long sessionStartTimeMillis = session.getStartTime();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(sessionStartTimeMillis);
        return calendar.get(HOUR_OF_DAY);
    }

    /**
     * Until Presto supports table generating functions, this is a workaround to support extracting multiple fields from a json string (similar to Hive's json_tuple)
     * Presto also doesn't support variable length arguments so keys to extract is a string representation of an array, e.g., ["key_1", "key_2"]
     */
    @ScalarFunction
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonExtractMultiple(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.VARCHAR) Slice keys)
    {
        requireNonNull(keys);
        if (json == null) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode newNode = mapper.createObjectNode();
        try {
            JsonNode rootNode = mapper.readTree(json.getInput());
            String[] keys2Extract = ArrayStringHelper.toStringArray(keys.toStringUtf8());
            for (String key : keys2Extract) {
                JsonNode node = rootNode.get(key);
                if (node != null) {
                    newNode.set(key, node);
                }
            }
        }
        catch (IOException e) {
            return null;
        }

        if (newNode.size() == 0) {
            return null;
        }
        else {
            return Slices.wrappedBuffer(newNode.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    @VisibleForTesting
    public static class ArrayStringHelper
    {
        /**
         * Input string has brackets and double quotes around individual array elements ["a", "b", "c.d"]
         *
         * @param arrayAsString string representation of an array of strings
         * @return a string array created from the given arrayAsString
         */
        public static String[] toStringArray(String arrayAsString)
        {
            int len = arrayAsString.length();
            if (arrayAsString.charAt(0) != '[' || arrayAsString.charAt(len - 1) != ']') {
                throw new IllegalArgumentException("Input string should have opening and closing brackets, e.g., [\"a\", \"b\", \"c.d\"]");
            }

            arrayAsString = arrayAsString.substring(1, len - 1);

            List<String> matches = new ArrayList<>();
            Pattern regex = Pattern.compile("\\s\"']+|\"[^\"]*\"");
            Matcher regexMatcher = regex.matcher(arrayAsString);
            while (regexMatcher.find()) {
                String stripped = regexMatcher.group().replaceAll("^\"|\"$", "");
                matches.add(stripped);
            }
            return matches.toArray(new String[matches.size()]);
        }
    }
}
