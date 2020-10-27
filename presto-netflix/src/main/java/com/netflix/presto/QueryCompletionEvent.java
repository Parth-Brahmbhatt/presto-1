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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryFailureInfo;
import io.prestosql.spi.eventlistener.QueryInputMetadata;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Immutable
@EventType("QueryCompletion")
public class QueryCompletionEvent
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String queryId;
    private final String transactionId;
    private final String user;
    private final String principal;
    private final String source;
    private final String resourceGroupName;
    private final String serverVersion;
    private final String environment;
    private final String catalog;
    private final String schema;
    private final String remoteClientAddress;
    private final String userAgent;
    private final String queryState;
    private final URI uri;
    private final List<String> fieldNames;
    private final String query;
    private final String plan;

    private final Long peakUserMemoryBytes;
    private final Long peakTotalMemoryBytes;
    private final Long peakTaskMemoryBytes;
    private final Double cumulutaiveMemoryBytes;

    private final DateTime createTime;
    private final DateTime executionStartTime;
    private final DateTime endTime;

    private final Long queuedTimeMs;
    private final Long analysisTimeMs;
    private final Long totalSplitWallTimeMs;
    private final Long totalSplitCpuTimeMs;
    private final Long totalBytes;
    private final Long totalRows;
    private final Long outputBytes;
    private final Long outputRows;
    private final Long writtenBytes;
    private final Long writtenRows;

    private final Integer splits;

    private final ErrorCode errorCode;
    private final String failureType;
    private final String failureMessage;
    private final String failureTask;
    private final String failureHost;

    private final String outputStageJson;
    private final String failuresJson;

    private final String inputsJson;
    private final String sessionPropertiesJson;

    public static QueryCompletionEvent fromQueryCompletedEvent(QueryCompletedEvent event)
            throws JsonProcessingException
    {
        final String queryId = event.getMetadata().getQueryId();
        final String transactionId = event.getMetadata().getTransactionId().orElse(null);
        final String user = event.getContext().getUser();
        final String principal = event.getContext().getPrincipal().orElse(null);
        final String source = event.getContext().getSource().orElse(null);
        final String serverVersion = event.getContext().getServerVersion();
        final String environment = event.getContext().getEnvironment();
        final String catalog = event.getContext().getCatalog().orElse(null);
        final String schema = event.getContext().getSchema().orElse(null);
        final String remoteClientAddress = event.getContext().getRemoteClientAddress().orElse(null);
        final String userAgent = event.getContext().getUserAgent().orElse(null);
        final String queryState = event.getMetadata().getQueryState();
        final URI uri = event.getMetadata().getUri();
        final String query = event.getMetadata().getQuery();
        final long peakMemoryBytes = event.getStatistics().getPeakUserMemoryBytes();
        final String plan = event.getMetadata().getPlan().orElse(null);
        final long peakUserMemoryBytes = event.getStatistics().getPeakUserMemoryBytes();
        final long peakTotalMemoryBytes = event.getStatistics().getPeakTotalNonRevocableMemoryBytes();
        final long peakTaskMemoryBytes = event.getStatistics().getPeakTaskTotalMemory();
        final double cumulativeUserMemoryBytes = event.getStatistics().getCumulativeMemory();
        final Instant createTime = event.getCreateTime();
        final Instant executionStartTime = event.getExecutionStartTime();
        final Instant endTime = event.getEndTime();
        final java.time.Duration queuedTime = event.getStatistics().getQueuedTime();
        final java.time.Duration analysisTime = event.getStatistics().getAnalysisTime().orElse(null);
        final java.time.Duration totalSplitWallTime = event.getStatistics().getWallTime();
        final java.time.Duration totalSplitCpuTime = event.getStatistics().getCpuTime();
        final long totalDataSize = event.getStatistics().getTotalBytes();
        final long totalRows = event.getStatistics().getTotalRows();
        final long outputDataSize = event.getStatistics().getOutputBytes();
        final long outputRows = event.getStatistics().getOutputRows();
        final long writtenDataSize = event.getStatistics().getWrittenBytes();
        final long writtenRows = event.getStatistics().getWrittenRows();
        int splits = event.getStatistics().getCompletedSplits();
        final ResourceGroupId resourceGroupId = event.getContext().getResourceGroupId().orElse(null);
        String resourceGroupName;
        if (resourceGroupId == null) {
            resourceGroupName = null;
        }
        else {
            resourceGroupName = resourceGroupId.toString();
        }
        ErrorCode errorCode = null;
        String failureType = null;
        String failureMessage = null;
        String failureTask = null;
        String failureHost = null;
        String failuresJson = null;
        if (event.getFailureInfo().isPresent()) {
            QueryFailureInfo failureInfo = event.getFailureInfo().get();
            errorCode = failureInfo.getErrorCode();
            failureType = failureInfo.getFailureType().orElse(null);
            failureMessage = failureInfo.getFailureMessage().orElse(null);
            failureTask = failureInfo.getFailureTask().orElse(null);
            failureHost = failureInfo.getFailureHost().orElse(null);
            failuresJson = failureInfo.getFailuresJson();
        }
        final String outputStageJson = null; // never used, filtered at side car level as well.
        final List<QueryInputMetadata> inputs = event.getIoMetadata().getInputs();
        final String inputsJson = inputs != null ? OBJECT_MAPPER.writeValueAsString(inputs) : null;
        List<String> fieldNames = inputs != null ? inputs.stream().map(inputMetadata -> inputMetadata.getColumns().stream().map(colNameType -> colNameType.contains(",") ? colNameType.split(",")[0] : colNameType).collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList()) : null;

        final String sessionPropertiesJson = OBJECT_MAPPER.writeValueAsString(event.getContext().getSessionProperties());

        return new QueryCompletionEvent(
                queryId,
                transactionId,
                user,
                principal,
                source,
                resourceGroupName,
                serverVersion,
                environment,
                catalog,
                schema,
                remoteClientAddress,
                userAgent,
                queryState,
                uri,
                fieldNames,
                query,
                plan,
                peakUserMemoryBytes,
                peakTotalMemoryBytes,
                peakTaskMemoryBytes,
                cumulativeUserMemoryBytes,
                createTime != null ? new DateTime(createTime.getEpochSecond() * 1000, DateTimeZone.UTC) : null,
                executionStartTime != null ? new DateTime(executionStartTime.getEpochSecond() * 1000, DateTimeZone.UTC) : null,
                endTime != null ? new DateTime(endTime.getEpochSecond() * 1000, DateTimeZone.UTC) : null,
                queuedTime != null ? new Duration(queuedTime.toMillis(), TimeUnit.MILLISECONDS) : null,
                analysisTime != null ? new Duration(analysisTime.toMillis(), TimeUnit.MILLISECONDS) : null,
                totalSplitWallTime != null ? new Duration(totalSplitWallTime.toMillis(), TimeUnit.MILLISECONDS) : null,
                totalSplitCpuTime != null ? new Duration(totalSplitCpuTime.toMillis(), TimeUnit.MILLISECONDS) : null,
                DataSize.of(totalDataSize, DataSize.Unit.BYTE),
                totalRows,
                DataSize.of(outputDataSize, DataSize.Unit.BYTE),
                outputRows,
                DataSize.of(writtenDataSize, DataSize.Unit.BYTE),
                writtenRows,
                splits,
                errorCode,
                failureType,
                failureMessage,
                failureTask,
                failureHost,
                outputStageJson,
                failuresJson,
                inputsJson,
                sessionPropertiesJson);
    }

    public QueryCompletionEvent(
            String queryId,
            String transactionId,
            String user,
            String principal,
            String source,
            String resourceGroupName,
            String serverVersion,
            String environment,
            String catalog,
            String schema,
            String remoteClientAddress,
            String userAgent,
            String queryState,
            URI uri,
            List<String> fieldNames,
            String query,
            String plan,
            Long peakUserMemoryBytes,
            Long peakTotalMemoryBytes,
            Long peakTaskMemoryBytes,
            Double cumulutaiveMemoryBytes,
            DateTime createTime,
            DateTime executionStartTime,
            DateTime endTime,
            Duration queuedTime,
            Duration analysisTime,
            Duration totalSplitWallTime,
            Duration totalSplitCpuTime,
            DataSize totalDataSize,
            Long totalRows,
            DataSize outputDataSize,
            Long outputRows,
            DataSize writtenDataSize,
            Long writtenRows,
            Integer splits,
            ErrorCode errorCode,
            String failureType,
            String failureMessage,
            String failureTask,
            String failureHost,
            String outputStageJson,
            String failuresJson,
            String inputsJson,
            String sessionPropertiesJson)
    {
        this.queryId = queryId;
        this.transactionId = transactionId;
        this.user = user;
        this.principal = principal;
        this.source = source;
        this.resourceGroupName = resourceGroupName;
        this.serverVersion = serverVersion;
        this.environment = environment;
        this.catalog = catalog;
        this.schema = schema;
        this.remoteClientAddress = remoteClientAddress;
        this.userAgent = userAgent;
        this.queryState = queryState;
        this.uri = uri;
        this.errorCode = errorCode;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.peakUserMemoryBytes = peakUserMemoryBytes;
        this.peakTotalMemoryBytes = peakTotalMemoryBytes;
        this.peakTaskMemoryBytes = peakTaskMemoryBytes;
        this.cumulutaiveMemoryBytes = cumulutaiveMemoryBytes;
        this.query = query;
        this.plan = plan;
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.endTime = endTime;
        this.queuedTimeMs = durationToMillis(queuedTime);
        this.analysisTimeMs = durationToMillis(analysisTime);
        this.totalSplitWallTimeMs = durationToMillis((totalSplitWallTime));
        this.totalSplitCpuTimeMs = durationToMillis(totalSplitCpuTime);
        this.totalBytes = sizeToBytes(totalDataSize);
        this.totalRows = totalRows;
        this.outputBytes = sizeToBytes(outputDataSize);
        this.outputRows = outputRows;
        this.writtenBytes = sizeToBytes(writtenDataSize);
        this.writtenRows = writtenRows;
        this.splits = splits;
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.failureTask = failureTask;
        this.failureHost = failureHost;
        this.outputStageJson = outputStageJson;
        this.failuresJson = failuresJson;
        this.inputsJson = inputsJson;
        this.sessionPropertiesJson = sessionPropertiesJson;
    }

    @Nullable
    private static Long durationToMillis(@Nullable Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    @Nullable
    private static Long sizeToBytes(@Nullable DataSize dataSize)
    {
        if (dataSize == null) {
            return null;
        }
        return dataSize.toBytes();
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getTransactionId()
    {
        return transactionId;
    }

    @EventField
    public String getUser()
    {
        return user;
    }

    @EventField
    public String getPrincipal()
    {
        return principal;
    }

    @EventField
    public String getSource()
    {
        return source;
    }

    @EventField
    public String getResourceGroupName()
    {
        return resourceGroupName;
    }

    @EventField
    public String getServerVersion()
    {
        return serverVersion;
    }

    @EventField
    public String getEnvironment()
    {
        return environment;
    }

    @EventField
    public String getCatalog()
    {
        return catalog;
    }

    @EventField
    public String getSchema()
    {
        return schema;
    }

    @EventField
    public String getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    @EventField
    public String getUserAgent()
    {
        return userAgent;
    }

    @EventField
    public String getQueryState()
    {
        return queryState;
    }

    @EventField
    public String getUri()
    {
        return uri.toString();
    }

    @EventField
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @EventField
    public String getQuery()
    {
        return query;
    }

    @EventField
    public String getPlan()
    {
        return plan;
    }

    @EventField
    public Long getPeakUserMemoryBytes()
    {
        return peakUserMemoryBytes;
    }

    @EventField
    public Long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @EventField
    public Long getPeakTaskMemoryBytes()
    {
        return peakTaskMemoryBytes;
    }

    @EventField
    public Double getCumulutaiveMemoryBytes()
    {
        return cumulutaiveMemoryBytes;
    }

    @EventField
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @EventField
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @EventField
    public DateTime getEndTime()
    {
        return endTime;
    }

    @EventField
    public Long getQueryWallTimeMs()
    {
        if (createTime == null || endTime == null) {
            return null;
        }
        return endTime.getMillis() - createTime.getMillis();
    }

    @EventField
    public Long getQueuedTimeMs()
    {
        return queuedTimeMs;
    }

    @EventField
    public Long getAnalysisTimeMs()
    {
        return analysisTimeMs;
    }

    @EventField
    public Long getTotalSplitWallTimeMs()
    {
        return totalSplitWallTimeMs;
    }

    @EventField
    public Long getTotalSplitCpuTimeMs()
    {
        return totalSplitCpuTimeMs;
    }

    @EventField
    public Long getBytesPerSec()
    {
        Long queryWallTimeMs = getQueryWallTimeMs();
        if (totalBytes == null || queryWallTimeMs == null) {
            return null;
        }
        return totalBytes * 1000 / (queryWallTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getBytesPerCpuSec()
    {
        if (totalBytes == null || totalSplitCpuTimeMs == null) {
            return null;
        }
        return totalBytes * 1000 / (totalSplitCpuTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getTotalBytes()
    {
        return totalBytes;
    }

    @EventField
    public Long getRowsPerSec()
    {
        Long queryWallTimeMs = getQueryWallTimeMs();
        if (totalRows == null || queryWallTimeMs == null) {
            return null;
        }
        return totalRows * 1000 / (queryWallTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getRowsPerCpuSec()
    {
        if (totalRows == null || totalSplitCpuTimeMs == null) {
            return null;
        }
        return totalRows * 1000 / (totalSplitCpuTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getTotalRows()
    {
        return totalRows;
    }

    @EventField
    public Long getOutputBytes()
    {
        return outputBytes;
    }

    @EventField
    public Long getOutputRows()
    {
        return outputRows;
    }

    @EventField
    public Long getWrittenBytes()
    {
        return writtenBytes;
    }

    @EventField
    public Long getWrittenRows()
    {
        return writtenRows;
    }

    @EventField
    public Integer getSplits()
    {
        return splits;
    }

    @EventField
    public Integer getErrorCode()
    {
        return errorCode == null ? null : errorCode.getCode();
    }

    @EventField
    public String getErrorCodeName()
    {
        return errorCode == null ? null : errorCode.getName();
    }

    @EventField
    public String getFailureType()
    {
        return failureType;
    }

    @EventField
    public String getFailureMessage()
    {
        return failureMessage;
    }

    @EventField
    public String getFailureTask()
    {
        return failureTask;
    }

    @EventField
    public String getFailureHost()
    {
        return failureHost;
    }

    @EventField
    public String getOutputStageJson()
    {
        return outputStageJson;
    }

    @EventField
    public String getFailuresJson()
    {
        return failuresJson;
    }

    @EventField
    public String getInputsJson()
    {
        return inputsJson;
    }

    @EventField
    public String getSessionPropertiesJson()
    {
        return sessionPropertiesJson;
    }
}
