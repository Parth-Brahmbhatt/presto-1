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
package io.prestosql.plugin.hive;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class PrestoHdfsCacheStats
{
    private final CounterStat fileReads = new CounterStat();
    private final CounterStat fileReadsFromS3 = new CounterStat();
    private final CounterStat fileReadsFromHDFS = new CounterStat();

    @Managed
    @Nested
    public CounterStat getFileReads()
    {
        return fileReads;
    }

    @Managed
    @Nested
    public CounterStat getFileReadsFromS3()
    {
        return fileReadsFromS3;
    }

    @Managed
    @Nested
    public CounterStat getFileReadsFromHDFS()
    {
        return fileReadsFromHDFS;
    }

    public void newFileRead()
    {
        fileReads.update(1);
    }

    public void newFileReadFromS3()
    {
        fileReadsFromS3.update(1);
    }

    public void newFileReadFromHDFS()
    {
        fileReadsFromHDFS.update(1);
    }
}
