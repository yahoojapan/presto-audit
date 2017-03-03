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
import io.airlift.log.Logger;
import org.json.simple.JSONObject;

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
        auditLogPath = requireNonNull(requiredConfig.get("event-listener.auditlog_path"), "event-listener.auditlog_path is null").toString();
        auditLogFileName = requireNonNull(requiredConfig.get("event-listener.auditlog_filename"), "event-listener.auditlog_filename is null").toString();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.debug("sso.output SQL BEGIN    : [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        log.debug("sso.output SQL COMPLETE : [ %s ]", queryCompletedEvent.getMetadata().getQuery());

        JSONObject obj = new JSONObject();

        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());

        obj.put("queryid", queryCompletedEvent.getMetadata().getQueryId());
        obj.put("query", queryCompletedEvent.getMetadata().getQuery());
        obj.put("uri", queryCompletedEvent.getMetadata().getUri().toString());
        obj.put("state", queryCompletedEvent.getMetadata().getQueryState());

        obj.put("cpuTime", String.valueOf(queryCompletedEvent.getStatistics().getCpuTime().toMillis() / 1000.0));
        obj.put("wallTime", String.valueOf(queryCompletedEvent.getStatistics().getWallTime().toMillis() / 1000.0));
        obj.put("queuedTime", String.valueOf(queryCompletedEvent.getStatistics().getQueuedTime().toMillis() / 1000.0));
        obj.put("peakMemoryBytes", String.valueOf(queryCompletedEvent.getStatistics().getPeakMemoryBytes()));
        obj.put("totalBytes", String.valueOf(queryCompletedEvent.getStatistics().getTotalBytes()));
        obj.put("totalRows", String.valueOf(queryCompletedEvent.getStatistics().getTotalRows()));

        obj.put("createTime", formatter.format(queryCompletedEvent.getCreateTime()));
        obj.put("executeStartTime", formatter.format(queryCompletedEvent.getExecutionStartTime()));

        obj.put("remoteClientAddress", queryCompletedEvent.getContext().getRemoteClientAddress().orElse(""));
        obj.put("clientUser", queryCompletedEvent.getContext().getUser());
        obj.put("userAgent", queryCompletedEvent.getContext().getUserAgent().orElse(""));
        obj.put("source", queryCompletedEvent.getContext().getSource().orElse(""));

        try (FileWriter file = new FileWriter(auditLogPath + File.separator + auditLogFileName, true)) {
            file.write(obj.toJSONString());
            file.write(System.lineSeparator());
        }
        catch (Exception e) {
            log.error("Error Write Eventlog to File. file path=" + auditLogPath + ", file name=" + auditLogFileName + ", Eventlog: " + obj);
        }
    }
}
