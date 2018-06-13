/**
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
import jp.co.yahoo.presto.audit.pulsar.PulsarProducer;
import jp.co.yahoo.presto.audit.serializer.FullLogSerializer;
import jp.co.yahoo.presto.audit.serializer.SerializedLog;
import jp.co.yahoo.presto.audit.serializer.SimpleLogSerializer;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
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
    private PulsarProducer pulsarSimpleProducer = null;
    private PulsarProducer pulsarFullProducer = null;

    @Inject
    public AuditLogListener(AuditConfig auditConfig)
        throws PulsarClientException
    {
        this.auditLogWriter = auditConfig.getAuditLogFileWriter();

        String auditLogPath = requireNonNull(auditConfig.getAuditLogPath(), "auditLogPath is null");
        String simpleLogName = requireNonNull(auditConfig.getAuditSimpleLogName(), "simpleLogName is null");
        Optional<String> fullLogName = Optional.ofNullable(auditConfig.getAuditFullLogName());
        this.simpleLogFilePath = auditLogPath + File.separator + simpleLogName;
        this.fullLogFilePath = fullLogName.map(s -> auditLogPath + File.separator + s);
        Optional<String> auditLogFullFilter = Optional.ofNullable(auditConfig.getLogFilter());

        Optional<String> simpleLogTopic = Optional.ofNullable(auditConfig.getSimpleTopic());
        Optional<String> fullLogTopic = Optional.ofNullable(auditConfig.getFullTopic());

        fullLogSerializer = new FullLogSerializer(auditLogFullFilter);
        simpleLogSerializer = new SimpleLogSerializer();

        if (simpleLogTopic.isPresent() || fullLogTopic.isPresent()) {
            String url = requireNonNull(auditConfig.getPulsarUrl());
            String trustCerts = requireNonNull(auditConfig.getTrustCertsPath());

            Map<String, String> authParams = new HashMap<String, String>();
            authParams.put("tenantDomain", requireNonNull(auditConfig.getTenantDomain()));
            authParams.put("tenantService", requireNonNull(auditConfig.getTenantService()));
            authParams.put("providerDomain", requireNonNull(auditConfig.getProviderDomain()));
            authParams.put("privateKey", requireNonNull(auditConfig.getPrivateKeyPath()));
            authParams.put("athenzConfPath", requireNonNull(auditConfig.getAthenzConfPath()));
            authParams.put("principalHeader", requireNonNull(auditConfig.getPrincipalHeader()));
            authParams.put("roleHeader", requireNonNull(auditConfig.getRoleHeader()));

            if (simpleLogTopic.isPresent()) {
                pulsarSimpleProducer = new PulsarProducer();
                pulsarSimpleProducer.initProducer(simpleLogTopic.get(), url, trustCerts, authParams);
            }

            if (fullLogTopic.isPresent()) {
                pulsarFullProducer = new PulsarProducer();
                pulsarFullProducer.initProducer(fullLogTopic.get(), url, trustCerts, authParams);
            }
        }
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
        SerializedLog simpleLog = simpleLogSerializer.serialize(queryCompletedEvent);
        auditLogWriter.write(simpleLogFilePath, simpleLog);
        if (pulsarSimpleProducer != null) {
            pulsarSimpleProducer.send(simpleLog);
        }
    }

    private void fullLog(QueryCompletedEvent queryCompletedEvent)
    {
        if (fullLogFilePath.isPresent() && fullLogSerializer.shouldOutput(queryCompletedEvent)) {
            try {
                SerializedLog fullLog = fullLogSerializer.serialize(queryCompletedEvent);
                auditLogWriter.write(fullLogFilePath.get(), fullLog);
                if (pulsarFullProducer != null) {
                    pulsarFullProducer.send(fullLog);
                }
            }
            catch (JsonProcessingException e) {
                log.error("Error in serializing full audit log: " + e.getMessage());
                log.error("Query failed: " + queryCompletedEvent.getMetadata().getQueryId());
            }
        }
    }
}
