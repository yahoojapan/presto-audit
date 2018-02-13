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
package jp.co.yahoo.presto.audit;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.airlift.log.Logger;

import java.io.File;
import java.io.FileWriter;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AuditLogListener
        implements EventListener
{
    private static final Logger log = Logger.get(AuditLogListener.class);

    private final String auditLogPath;
    private final String auditLogFileName;

    public AuditLogListener(Map<String, String> requiredConfig)
    {
        auditLogPath = requireNonNull(requiredConfig.get("event-listener.audit-log-path"), "event-listener.audit-log-path is null");
        auditLogFileName = requireNonNull(requiredConfig.get("event-listener.audit-log-filename"), "event-listener.audit-log-filename is null");
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.debug("QUERY SQL : [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        AuditRecord record = buildAuditRecord(queryCompletedEvent);

        Gson obj = new GsonBuilder().disableHtmlEscaping().create();
        try (FileWriter file = new FileWriter(auditLogPath + File.separator + auditLogFileName, true)) {
            file.write(obj.toJson(record));
            file.write(System.lineSeparator());
        }
        catch (Exception e) {
            log.error("Error writing event log to file. ErrorMessage:" + e.getMessage());
            log.error("EventLog write failed:" + obj.toJson(record));
        }
    }

    AuditRecord buildAuditRecord(QueryCompletedEvent event)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS").withZone(ZoneId.systemDefault());

        AuditRecord record = new AuditRecord();
        record.setEventType("QueryCompletedEvent");
        record.setQueryId(event.getMetadata().getQueryId());

        //SQL Query Text
        record.setQuery(event.getMetadata().getQuery());
        record.setUri(event.getMetadata().getUri().toString());
        record.setState(event.getMetadata().getQueryState());

        record.setCpuTime(event.getStatistics().getCpuTime().toMillis() / 1000.0);
        record.setWallTime(event.getStatistics().getWallTime().toMillis() / 1000.0);
        record.setQueuedTime(event.getStatistics().getQueuedTime().toMillis() / 1000.0);
        record.setPeakMemoryBytes(event.getStatistics().getPeakMemoryBytes());
        record.setTotalBytes(event.getStatistics().getTotalBytes());
        record.setTotalRows(event.getStatistics().getTotalRows());
        record.setCompletedSplits(event.getStatistics().getCompletedSplits());

        record.setCreateTime(formatter.format(event.getCreateTime()));
        record.setExecutionStartTime(formatter.format(event.getExecutionStartTime()));
        record.setEndTime(formatter.format(event.getEndTime()));

        record.setCreateTimestamp(event.getCreateTime().toEpochMilli() / 1000.0);
        record.setExecutionStartTimestamp(event.getExecutionStartTime().toEpochMilli() / 1000.0);
        record.setEndTimestamp(event.getEndTime().toEpochMilli() / 1000.0);

        // Error information
        if (event.getFailureInfo().isPresent()) {
            QueryFailureInfo failureInfo = event.getFailureInfo().get();
            record.setErrorCode(failureInfo.getErrorCode().getCode());
            record.setErrorName(failureInfo.getErrorCode().getName());
            if (failureInfo.getFailureType().isPresent()) {
                record.setFailureType(failureInfo.getFailureType().get());
            }
            if (failureInfo.getFailureMessage().isPresent()) {
                record.setFailureMessage(failureInfo.getFailureMessage().get());
            }
            record.setFailuresJson(failureInfo.getFailuresJson());
        }

        record.setRemoteClientAddress(event.getContext().getRemoteClientAddress().orElse(""));
        record.setClientUser(event.getContext().getUser());
        record.setUserAgent(event.getContext().getUserAgent().orElse(""));
        record.setSource(event.getContext().getSource().orElse(""));
        return record;
    }
}
