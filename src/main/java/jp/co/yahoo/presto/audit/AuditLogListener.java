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
import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;
import jp.co.yahoo.presto.audit.serializer.FullLogSerializer;
import jp.co.yahoo.presto.audit.serializer.SimpleLogSerializer;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AuditLogListener
        implements EventListener
{
    private static final Logger log = Logger.get(AuditLogListener.class);
    private final AuditLogFileWriter auditLogWriter;

    private final String simpleLogFilePath;
    private final Optional<String> fullLogFilePath;

    private final FullLogSerializer fullLogSerializer;
    private final SimpleLogSerializer simpleLogSerializer;

    public AuditLogListener(Map<String, String> requiredConfig, AuditLogFileWriter auditLogWriter)
    {
        String auditLogPath = requireNonNull(requiredConfig.get("event-listener.audit-log-path"), "event-listener.audit-log-path is null");
        simpleLogFilePath = auditLogPath + File.separator + requireNonNull(requiredConfig.get("event-listener.audit-log-filename"), "event-listener.audit-log-filename is null");

        // Only if audit-log-full-filename exist, then output full log
        Optional<String> auditLogFullFileName = Optional.ofNullable(requiredConfig.get("event-listener.audit-log-full-filename"));
        fullLogFilePath = auditLogFullFileName.isPresent() ? Optional.of(auditLogPath + File.separator + auditLogFullFileName.get()) : Optional.empty();

        // Initialize file writer
        this.auditLogWriter = auditLogWriter;

        fullLogSerializer = new FullLogSerializer();
        simpleLogSerializer = new SimpleLogSerializer();
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        log.debug("QUERY SQL : [ %s ]", queryCreatedEvent.getMetadata().getQuery());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        simpleLog(queryCompletedEvent);
        fullLog(queryCompletedEvent);
    }

    private void simpleLog(QueryCompletedEvent queryCompletedEvent)
    {
        auditLogWriter.write(simpleLogFilePath, simpleLogSerializer.serialize(queryCompletedEvent));
    }

    private void fullLog(QueryCompletedEvent queryCompletedEvent)
    {
        if (fullLogFilePath.isPresent()) {
            try {
                auditLogWriter.write(fullLogFilePath.get(), fullLogSerializer.serialize(queryCompletedEvent));
            }
            catch (JsonProcessingException e) {
                log.error("Error in serializing full audit log: " + e.getMessage());
                log.error("Query failed: " + queryCompletedEvent.getMetadata().getQueryId());
            }
        }
    }
}
