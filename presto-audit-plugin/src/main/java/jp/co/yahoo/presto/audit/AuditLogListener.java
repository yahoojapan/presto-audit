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
import com.google.gson.Gson;
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
        auditLogPath = requireNonNull(requiredConfig.get("event-listener.auditlog-path"), "event-listener.auditlog-path is null").toString();
        auditLogFileName = requireNonNull(requiredConfig.get("event-listener.auditlog-filename"), "event-listener.auditlog-filename is null").toString();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.debug("QUERY SQL : [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS").withZone(ZoneId.systemDefault());

        AuditRecord record = new AuditRecord();
        record.setQueryId(queryCompletedEvent.getMetadata().getQueryId());

        //SQL Query Text
        record.setQuery(queryCompletedEvent.getMetadata().getQuery());
        record.setUri(queryCompletedEvent.getMetadata().getUri().toString());
        record.setState(queryCompletedEvent.getMetadata().getQueryState());

        record.setCpuTime(queryCompletedEvent.getStatistics().getCpuTime().toMillis() / 1000.0);
        record.setWallTime((queryCompletedEvent.getEndTime().toEpochMilli() - queryCompletedEvent.getExecutionStartTime().toEpochMilli()) / 1000.0);
        record.setQueuedTime(queryCompletedEvent.getStatistics().getQueuedTime().toMillis() / 1000.0);
        record.setPeakMemoryBytes(queryCompletedEvent.getStatistics().getPeakMemoryBytes());
        record.setTotalBytes(queryCompletedEvent.getStatistics().getTotalBytes());
        record.setTotalRows(queryCompletedEvent.getStatistics().getTotalRows());

        record.setCreateTime(formatter.format(queryCompletedEvent.getCreateTime()));
        record.setExecuteStartTime(formatter.format(queryCompletedEvent.getExecutionStartTime()));
        record.setEndTime(formatter.format(queryCompletedEvent.getEndTime()));

        record.setRemoteClientAddress(queryCompletedEvent.getContext().getRemoteClientAddress().orElse(""));
        record.setClientUser(queryCompletedEvent.getContext().getUser());
        record.setUserAgent(queryCompletedEvent.getContext().getUserAgent().orElse(""));
        record.setSource(queryCompletedEvent.getContext().getSource().orElse(""));

        Gson obj = new Gson();
        try (FileWriter file = new FileWriter(auditLogPath + File.separator + auditLogFileName, true)) {
            file.write(obj.toJson(record));
            file.write(System.lineSeparator());
        }
        catch (Exception e) {
            log.error("Error Write EventLog to File. file path=" + auditLogPath + ", file name=" + auditLogFileName + ", EventLog: " + obj);
        }
    }
}
